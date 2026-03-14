use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use rusqlite::Connection;
use serde::Deserialize;

use crate::app::{
    ImportedStation,
    schedule::{
        CityInformation, CityName, FailureReason, GeospatialMappingFailure,
        GeospatialMappingResult, GeospatialRepository,
    },
};

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
struct NominatimAddress {
    city: Option<String>,
    town: Option<String>,
    village: Option<String>,
    hamlet: Option<String>,
    country: Option<String>,
    municipality: Option<String>,
}

#[derive(Deserialize, Debug)]
struct NominatimResponse {
    lat: String,
    lon: String,
    address: NominatimAddress,
}

#[derive(Clone)]
pub struct NominatimGeospatialRepository {
    client: reqwest::Client,
    base_url: String,
    cache: Arc<Mutex<Connection>>,
}

impl NominatimGeospatialRepository {
    /// Open (or create) a persistent geocode cache at `cache_path`.
    /// Pass `":memory:"` for a transient in-process cache (useful in tests).
    pub fn new(base_url: &str, cache_path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(cache_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS geocode_cache (
                lat             REAL NOT NULL,
                lon             REAL NOT NULL,
                city            TEXT NOT NULL,
                country         TEXT NOT NULL,
                municipality    TEXT,
                city_lat        REAL NOT NULL,
                city_lon        REAL NOT NULL,
                PRIMARY KEY (lat, lon)
            );",
        )?;
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.to_string(),
            cache: Arc::new(Mutex::new(conn)),
        })
    }

    fn lookup_cache(&self, lat: f64, lon: f64) -> Option<CityInformation> {
        let conn = self.cache.lock().ok()?;
        conn.query_row(
            "SELECT city, country, city_lat, city_lon
                FROM geocode_cache
                WHERE lat = ?1 AND lon = ?2",
            rusqlite::params![lat, lon],
            |row| {
                let city: String = row.get(0)?;
                let country: String = row.get(1)?;
                let city_lat: f64 = row.get(2)?;
                let city_lon: f64 = row.get(3)?;
                Ok(CityInformation::new(
                    city.into(),
                    country.into(),
                    city_lat,
                    city_lon,
                ))
            },
        )
        .ok()
    }

    fn store_cache(&self, lat: f64, lon: f64, info: &CityInformation) {
        if let Ok(conn) = self.cache.lock() {
            let _ = conn.execute(
                "INSERT OR IGNORE INTO geocode_cache
                    (lat, lon, city, country, city_lat, city_lon)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    lat,
                    lon,
                    info.name(),
                    info.country(),
                    info.lat(),
                    info.lon(),
                ],
            );
        }
    }

    async fn reverse_geocode(&self, lat: f64, lon: f64) -> Result<CityInformation, FailureReason> {
        if let Some(cached) = self.lookup_cache(lat, lon) {
            return Ok(cached);
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
            .await
            .map_err(|_| FailureReason::NetworkError)?;

        if !response.status().is_success() {
            let status_code = response.status().as_u16();
            tracing::warn!(
                lat,
                lon,
                status = status_code,
                "Nominatim reverse geocoding failed"
            );
            return Err(FailureReason::HttpError { status_code });
        }

        let nominatim: NominatimResponse = response
            .json()
            .await
            .map_err(|_| FailureReason::NetworkError)?;
        let city_lat = nominatim
            .lat
            .parse::<f64>()
            .map_err(|_| FailureReason::InvalidCoordinates)?;
        let city_lon = nominatim
            .lon
            .parse::<f64>()
            .map_err(|_| FailureReason::InvalidCoordinates)?;
        let addr = nominatim.address;
        let city_name = extract_city_name(&addr).ok_or(FailureReason::MissingCityData)?;
        let country = addr.country.ok_or(FailureReason::MissingCityData)?.into();

        let info = CityInformation::new(city_name, country, city_lat, city_lon);
        self.store_cache(lat, lon, &info);
        Ok(info)
    }
}

fn extract_city_name(addr: &NominatimAddress) -> Option<CityName> {
    addr.city
        .clone()
        .or_else(|| addr.town.clone())
        .or_else(|| addr.village.clone())
        .or_else(|| addr.hamlet.clone())
        .or_else(|| addr.municipality.clone())
        .map(CityName::from)
}

impl GeospatialRepository for NominatimGeospatialRepository {
    async fn match_stations_to_cities(
        &self,
        stations: &[ImportedStation],
    ) -> GeospatialMappingResult {
        let mut mapping = HashMap::new();
        let mut failures = Vec::new();

        for station in stations {
            match self.reverse_geocode(station.lat(), station.lon()).await {
                Ok(info) => {
                    mapping.insert(station.id().clone(), info);
                }
                Err(reason) => {
                    tracing::warn!(
                        station_id = station.id().as_str(),
                        name = station.name(),
                        reason = ?reason,
                        "Could not resolve city for station"
                    );
                    failures.push(GeospatialMappingFailure {
                        station_id: station.id().clone(),
                        station_name: station.name().to_string(),
                        lat: station.lat(),
                        lon: station.lon(),
                        reason,
                    });
                }
            }
        }

        GeospatialMappingResult { mapping, failures }
    }
}

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use crate::app::ImportedStationId;

    use super::*;

    fn addr(
        city: Option<&str>,
        town: Option<&str>,
        village: Option<&str>,
        hamlet: Option<&str>,
        municipality: Option<&str>,
    ) -> NominatimAddress {
        NominatimAddress {
            city: city.map(str::to_owned),
            town: town.map(str::to_owned),
            village: village.map(str::to_owned),
            hamlet: hamlet.map(str::to_owned),
            municipality: municipality.map(str::to_owned),
            country: None,
        }
    }

    // ---- extract_city_name ----

    #[test]
    fn city_takes_priority_over_all_others() {
        let a = addr(
            Some("Paris"),
            Some("Le Perreux"),
            Some("v"),
            Some("h"),
            Some("m"),
        );
        assert_eq!(extract_city_name(&a), Some("Paris".into()));
    }

    #[test]
    fn town_used_when_no_city() {
        let a = addr(None, Some("Mâcon"), None, None, None);
        assert_eq!(extract_city_name(&a), Some("Mâcon".into()));
    }

    #[test]
    fn village_used_when_no_city_or_town() {
        let a = addr(None, None, Some("Loché"), None, None);
        assert_eq!(extract_city_name(&a), Some("Loché".into()));
    }

    #[test]
    fn hamlet_used_when_no_city_town_or_village() {
        let a = addr(None, None, None, Some("Petit Hamlet"), None);
        assert_eq!(extract_city_name(&a), Some("Petit Hamlet".into()));
    }

    #[test]
    fn municipality_used_when_all_higher_priority_absent() {
        let a = addr(None, None, None, None, Some("Test Municipality"));
        assert_eq!(extract_city_name(&a), Some("Test Municipality".into()));
    }

    #[test]
    fn returns_none_when_all_fields_absent() {
        let a = addr(None, None, None, None, None);
        assert_eq!(extract_city_name(&a), None);
    }

    // ---- reverse_geocode (HTTP) ----

    fn nominatim_body(city: &str, country: &str) -> String {
        format!(
            r#"{{"lat":"45.75","lon":"4.85","address":{{"city":"{city}","country":"{country}"}}}}"#
        )
    }

    #[tokio::test]
    async fn reverse_geocode_returns_city_on_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.reverse_geocode(45.75, 4.85).await.unwrap();

        assert_eq!(result.name(), &"Lyon".into());
        assert_eq!(result.country(), &"France".into());
        assert_eq!(result.lat(), 45.75);
        assert_eq!(result.lon(), 4.85);
        mock.assert();
    }

    #[tokio::test]
    async fn reverse_geocode_returns_none_on_http_error() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(500)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.reverse_geocode(45.75, 4.85).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            FailureReason::HttpError { status_code: 500 }
        );
    }

    #[tokio::test]
    async fn reverse_geocode_returns_none_when_city_missing_from_address() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(r#"{"lat":"45.75","lon":"4.85","address":{"country":"France"}}"#)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.reverse_geocode(45.75, 4.85).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), FailureReason::MissingCityData);
    }

    #[tokio::test]
    async fn cache_hit_skips_http_call() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(1)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let first = repo.reverse_geocode(45.75, 4.85).await.unwrap();
        let second = repo.reverse_geocode(45.75, 4.85).await.unwrap();

        assert_eq!(first.name(), second.name());
        mock.assert();
    }

    #[tokio::test]
    async fn cache_hit_preserves_city_coordinates() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(1)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let _ = repo.reverse_geocode(45.70, 4.80).await.unwrap();
        let cached = repo.reverse_geocode(45.70, 4.80).await.unwrap();

        assert_eq!(cached.lat(), 45.75);
        assert_eq!(cached.lon(), 4.85);
    }

    #[tokio::test]
    async fn reverse_geocode_returns_none_when_lat_lon_unparseable() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(r#"{"lat":"N/A","lon":"N/A","address":{"city":"Lyon","country":"France"}}"#)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.reverse_geocode(45.75, 4.85).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), FailureReason::InvalidCoordinates);
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

    #[tokio::test]
    async fn match_stations_skips_unresolvable_stations() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(500)
            .create_async()
            .await;

        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo
            .match_stations_to_cities(&[station("A", 0.0, 0.0)])
            .await;

        assert!(result.mapping.is_empty());
        assert_eq!(result.failures.len(), 1);
        assert_eq!(
            result.failures[0].station_id,
            ImportedStationId::from("A".to_owned())
        );
        assert_eq!(
            result.failures[0].reason,
            FailureReason::HttpError { status_code: 500 }
        );
    }

    #[tokio::test]
    async fn match_stations_keys_by_station_id() {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/reverse")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_header("Content-Type", "application/json")
            .with_body(nominatim_body("Lyon", "France"))
            .expect(2)
            .create_async()
            .await;

        let stations = vec![station("A", 45.75, 4.85), station("B", 45.76, 4.86)];
        let repo = NominatimGeospatialRepository::new(&server.url(), ":memory:").unwrap();
        let result = repo.match_stations_to_cities(&stations).await;

        assert_eq!(result.mapping.len(), 2);
        assert!(result.failures.is_empty());
        assert!(
            result
                .mapping
                .contains_key(&ImportedStationId::from("A".to_owned()))
        );
        assert!(
            result
                .mapping
                .contains_key(&ImportedStationId::from("B".to_owned()))
        );
        assert_eq!(
            result.mapping.get(&ImportedStationId::from("A".to_owned())),
            result.mapping.get(&ImportedStationId::from("B".to_owned()))
        );
    }
}
