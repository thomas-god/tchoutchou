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
    municipality: Option<String>,
    country: Option<String>,
}

#[derive(Deserialize, Debug)]
struct NominatimResponse {
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
                lat  REAL NOT NULL,
                lon  REAL NOT NULL,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
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
            "SELECT city, country FROM geocode_cache WHERE lat = ?1 AND lon = ?2",
            rusqlite::params![lat, lon],
            |row| {
                let city: String = row.get(0)?;
                let country: String = row.get(1)?;
                Ok(CityInformation::new(city, country))
            },
        )
        .ok()
    }

    fn store_cache(&self, lat: f64, lon: f64, info: &CityInformation) {
        if let Ok(conn) = self.cache.lock() {
            let _ = conn.execute(
                "INSERT OR IGNORE INTO geocode_cache (lat, lon, city, country) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![lat, lon, info.name(), info.country()],
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
                ("zoom", "8".to_string()),
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
        let addr = nominatim.address;
        let city_name = extract_city_name(&addr)?;
        let country = addr.country?;

        let info = CityInformation::new(city_name, country);
        self.store_cache(lat, lon, &info);
        Some(info)
    }
}

fn extract_city_name(addr: &NominatimAddress) -> Option<String> {
    addr.city
        .clone()
        .or_else(|| addr.town.clone())
        .or_else(|| addr.village.clone())
        .or_else(|| addr.municipality.clone())
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

    fn addr(
        city: Option<&str>,
        town: Option<&str>,
        village: Option<&str>,
        municipality: Option<&str>,
    ) -> NominatimAddress {
        NominatimAddress {
            city: city.map(str::to_owned),
            town: town.map(str::to_owned),
            village: village.map(str::to_owned),
            municipality: municipality.map(str::to_owned),
            country: None,
        }
    }

    // ---- extract_city_name ----

    #[test]
    fn city_takes_priority_over_all_others() {
        let a = addr(Some("Paris"), Some("Le Perreux"), Some("v"), Some("m"));
        assert_eq!(extract_city_name(&a).as_deref(), Some("Paris"));
    }

    #[test]
    fn town_used_when_no_city() {
        let a = addr(None, Some("Mâcon"), None, None);
        assert_eq!(extract_city_name(&a).as_deref(), Some("Mâcon"));
    }

    #[test]
    fn village_used_when_no_city_or_town() {
        let a = addr(None, None, Some("Loché"), None);
        assert_eq!(extract_city_name(&a).as_deref(), Some("Loché"));
    }

    #[test]
    fn municipality_used_as_last_resort() {
        let a = addr(None, None, None, Some("Grand Lyon"));
        assert_eq!(extract_city_name(&a).as_deref(), Some("Grand Lyon"));
    }

    #[test]
    fn returns_none_when_all_fields_absent() {
        let a = addr(None, None, None, None);
        assert_eq!(extract_city_name(&a), None);
    }

    // ---- reverse_geocode (HTTP) ----

    fn nominatim_body(city: &str, country: &str) -> String {
        format!(r#"{{"address":{{"city":"{city}","country":"{country}"}}}}"#)
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
}
