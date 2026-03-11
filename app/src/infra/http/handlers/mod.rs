use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    domain::optim::{City, CityId, DestinationFilters},
    infra::http::AppState,
};

// --- cities autocomplete ---

#[derive(Deserialize)]
pub struct QueryParameters {
    substring: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteCityResponseItem {
    name: String,
    id: i64,
}

impl From<City> for AutocompleteCityResponseItem {
    fn from(value: City) -> Self {
        Self {
            name: value.name().to_owned(),
            id: value.id().as_i64(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteCityResponse {
    pub stations: Vec<AutocompleteCityResponseItem>,
}

pub async fn autocomplete_city(
    State(state): State<AppState>,
    Query(QueryParameters { substring }): Query<QueryParameters>,
) -> Result<Json<AutocompleteCityResponse>, StatusCode> {
    let Ok(cities) = state.schedule.search_cities_by_name(&substring, 10) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    Ok(Json(AutocompleteCityResponse {
        stations: cities
            .into_iter()
            .map(AutocompleteCityResponseItem::from)
            .collect(),
    }))
}

// --- destinations search ---

#[derive(Deserialize)]
pub struct DestinationsQueryParameters {
    from: i64,
    max_connections: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationResponseItem {
    station_id: i64,
    duration: usize,
    connections: usize,
    visited_station_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CityResponseItem {
    id: i64,
    name: String,
    country: String,
    lat: f64,
    lon: f64,
}

impl From<City> for CityResponseItem {
    fn from(city: City) -> Self {
        Self {
            id: city.id().as_i64(),
            name: city.name().to_owned(),
            country: city.country().to_owned(),
            lat: city.lat(),
            lon: city.lon(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationsResponse {
    destinations: Vec<DestinationResponseItem>,
    cities: Vec<CityResponseItem>,
}

pub async fn get_destinations(
    State(state): State<AppState>,
    Query(DestinationsQueryParameters {
        from,
        max_connections,
    }): Query<DestinationsQueryParameters>,
) -> Result<Json<DestinationsResponse>, StatusCode> {
    let date = Utc::now().date_naive().format("%Y%m%d").to_string();

    let origin = CityId::from(from);
    let filters = DestinationFilters::new(max_connections.unwrap_or(1), 900, 3600 * 12);
    let (destinations, cities) = state
        .schedule
        .find_destinations(&date, &origin, &filters)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = destinations
        .into_iter()
        .map(|d| DestinationResponseItem {
            station_id: d.destination(),
            duration: d.duration(),
            connections: d.number_of_connections(),
            visited_station_ids: d
                .intermediary_city_ids()
                .iter()
                .map(|s| s.as_i64())
                .collect(),
        })
        .collect();

    let city_items = cities.into_iter().map(CityResponseItem::from).collect();

    Ok(Json(DestinationsResponse {
        destinations: items,
        cities: city_items,
    }))
}
