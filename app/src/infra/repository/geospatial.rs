use std::{collections::HashMap, sync::Mutex};

use rusqlite::Connection;
use serde::Deserialize;

use crate::app::schedulev2::{
    CityInformation, GeospatialRepository, ImportedStation, ImportedStationId,
};

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
struct NominatimAddress {
    city: Option<String>,
    town: Option<String>,
    village: Option<String>,
    country: Option<String>,
}

#[derive(Deserialize, Debug)]
struct NominatimResponse {
    lat: String,
    lon: String,
    address: NominatimAddress,
}

pub struct NominatimGeospatialRepository {
    client: reqwest::blocking::Client,
    base_url: String,
    cache: Mutex<Connection>,
}

impl NominatimGeospatialRepository {
    /// Open (or create) a persistent geocode cache at `cache_path`.
    /// Pass `":memory:"` for a transient in-process cache (useful in tests).
    pub fn new(base_url: &str, cache_path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(cache_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS geocode_cache (
                lat      REAL NOT NULL,
                lon      REAL NOT NULL,
                city     TEXT NOT NULL,
                country  TEXT NOT NULL,
                city_lat REAL NOT NULL,
                city_lon REAL NOT NULL,
                PRIMARY KEY (lat, lon)
            );",
        )?;
        Ok(Self {
            client: reqwest::blocking::Client::new(),
            base_url: base_url.to_string(),
            cache: Mutex::new(conn),
        })
    }

    fn lookup_cache(&self, lat: f64, lon: f64) -> Option<CityInformation> {
        let conn = self.cache.lock().ok()?;
        conn.query_row(
            "SELECT city, country, city_lat, city_lon FROM geocode_cache WHERE lat = ?1 AND lon = ?2",
            rusqlite::params![lat, lon],
            |row| {
                let city: String = row.get(0)?;
                let country: String = row.get(1)?;
                let city_lat: f64 = row.get(2)?;
                let city_lon: f64 = row.get(3)?;
                Ok(CityInformation::new(city, country, city_lat, city_lon))
            },
        )
        .ok()
    }

    fn store_cache(&self, lat: f64, lon: f64, info: &CityInformation) {
        if let Ok(conn) = self.cache.lock() {
            let _ = conn.execute(
                "INSERT OR IGNORE INTO geocode_cache (lat, lon, city, country, city_lat, city_lon) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![lat, lon, info.name(), info.country(), info.lat(), info.lon()],
            );
        }
    }

    fn reverse_geocode(&self, lat: f64, lon: f64) -> Option<CityInformation> {
        if let Some(cached) = self.lookup_cache(lat, lon) {
            return Some(cached);
        }

        let url = format!("{}/reverse", self.base_url.trim_end_matches('/'));
        let response = self
            .client
            .get(&url)
            .query(&[
                ("lat", lat.to_string()),
                ("lon", lon.to_string()),
                ("format", "json".to_string()),
                ("zoom", "10".to_string()),
                ("addressdetails", "1".to_string()),
            ])
            .send()
            .ok()?;

        if !response.status().is_success() {
            tracing::warn!(
                lat,
                lon,
                status = response.status().as_u16(),
                "Nominatim reverse geocoding failed"
            );
            return None;
        }

        let nominatim: NominatimResponse = response.json().ok()?;
        let city_lat = nominatim.lat.parse::<f64>().ok()?;
        let city_lon = nominatim.lon.parse::<f64>().ok()?;
        let addr = nominatim.address;
        let city_name = extract_city_name(&addr)?;
        let country = addr.country?;

        let info = CityInformation::new(city_name, country, city_lat, city_lon);
        self.store_cache(lat, lon, &info);
        Some(info)
    }
}

fn extract_city_name(addr: &NominatimAddress) -> Option<String> {
    addr.city
        .clone()
        .or_else(|| addr.town.clone())
        .or_else(|| addr.village.clone())
}

impl GeospatialRepository for NominatimGeospatialRepository {
    fn match_stations_to_cities(
        &self,
        stations: &[ImportedStation],
    ) -> HashMap<ImportedStationId, CityInformation> {
        let mut result = HashMap::new();

        for station in stations {
            let city = self.reverse_geocode(station.lat(), station.lon());

            if let Some(info) = city {
                result.insert(station.id().clone(), info);
            } else {
                tracing::warn!(
                    station_id = station.id().as_str(),
                    name = station.name(),
                    "Could not resolve city for station"
                );
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use super::*;

    fn addr(city: Option<&str>, town: Option<&str>, village: Option<&str>) -> NominatimAddress {
        NominatimAddress {
            city: city.map(str::to_owned),
            town: town.map(str::to_owned),
            village: village.map(str::to_owned),
            country: None,
        }
    }

    // ---- extract_city_name ----

    #[test]
    fn city_takes_priority_over_all_others() {
        let a = addr(Some("Paris"), Some("Le Perreux"), Some("v"));
        assert_eq!(extract_city_name(&a).as_deref(), Some("Paris"));
    }

    #[test]
    fn town_used_when_no_city() {
        let a = addr(None, Some("Mâcon"), None);
        assert_eq!(extract_city_name(&a).as_deref(), Some("Mâcon"));
    }

    #[test]
    fn village_used_when_no_city_or_town() {
        let a = addr(None, None, Some("Loché"));
        assert_eq!(extract_city_name(&a).as_deref(), Some("Loché"));
    }

    #[test]
    fn returns_none_when_all_fields_absent() {
        let a = addr(None, None, None);
        assert_eq!(extract_city_name(&a), None);
    }

    // ---- reverse_geocode (HTTP) ----

    fn nominatim_body(city: &str, country: &str) -> String {
        format!(
            r#"{{"lat":"45.75","lon":"4.85","address":{{"city":"{city}","country":"{country}"}}}}"#
        )
    }

    #[test]
    fn reverse_geocode_returns_city_on_success() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.reverse_geocode(45.75, 4.85).unwrap();

        assert_eq!(result.name(), "Lyon");
        assert_eq!(result.country(), "France");
        assert_eq!(result.lat(), 45.75);
        assert_eq!(result.lon(), 4.85);
        mock.assert();
    }

    #[test]
    fn reverse_geocode_returns_none_on_http_error() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(500)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        assert!(repo.reverse_geocode(45.75, 4.85).is_none());
    }

    #[test]
    fn reverse_geocode_returns_none_when_city_missing_from_address() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(r#"{"address":{"country":"France"}}"#)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        assert!(repo.reverse_geocode(45.75, 4.85).is_none());
    }

    #[test]
    fn cache_hit_skips_http_call() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(1)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let first = repo.reverse_geocode(45.75, 4.85).unwrap();
        let second = repo.reverse_geocode(45.75, 4.85).unwrap();

        assert_eq!(first.name(), second.name());
        mock.assert();
    }

    #[test]
    fn cache_hit_preserves_city_coordinates() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(1)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let _ = repo.reverse_geocode(45.70, 4.80).unwrap();
        let cached = repo.reverse_geocode(45.70, 4.80).unwrap();

        assert_eq!(cached.lat(), 45.75);
        assert_eq!(cached.lon(), 4.85);
    }

    #[test]
    fn reverse_geocode_returns_none_when_lat_lon_unparseable() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(r#"{"lat":"N/A","lon":"N/A","address":{"city":"Lyon","country":"France"}}"#)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        assert!(repo.reverse_geocode(45.75, 4.85).is_none());
    }

    // ---- match_stations_to_cities ----

    fn station(id: &str, lat: f64, lon: f64) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            lat,
            lon,
        )
    }

    #[test]
    fn match_stations_skips_unresolvable_stations() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(500)
            .create();

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.match_stations_to_cities(&[station("A", 0.0, 0.0)]);

        assert!(result.is_empty());
    }

    #[test]
    fn match_stations_keys_by_station_id() {
        let mut server = Server::new();
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(2)
            .create();

        let stations = vec![station("A", 45.75, 4.85), station("B", 45.76, 4.86)];
        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.match_stations_to_cities(&stations);

        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&ImportedStationId::from("A".to_owned())));
        assert!(result.contains_key(&ImportedStationId::from("B".to_owned())));
        assert_eq!(
            result.get(&ImportedStationId::from("A".to_owned())),
            result.get(&ImportedStationId::from("B".to_owned()))
        );
    }
}
