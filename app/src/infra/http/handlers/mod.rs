use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    app::schedule::CityWithExtraInformation,
    domain::{City, CityId},
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
            name: value.name().to_string(),
            id: **value.id(),
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
            id: **city.id(),
            name: city.name().to_string(),
            country: city.country().to_string(),
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

// --- all cities ---

#[derive(Debug, Clone, Serialize)]
pub struct CityWithExtraInformationResponseItem {
    id: i64,
    name: String,
    country: String,
    lat: f64,
    lon: f64,
    wikidata: Option<String>,
    wikipedia: Option<String>,
}

impl From<CityWithExtraInformation> for CityWithExtraInformationResponseItem {
    fn from(c: CityWithExtraInformation) -> Self {
        Self {
            id: **c.city.id(),
            name: c.city.name().to_string(),
            country: c.city.country().to_string(),
            lat: c.city.lat(),
            lon: c.city.lon(),
            wikidata: c.wikidata,
            wikipedia: c.wikipedia,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CitiesResponse {
    cities: Vec<CityWithExtraInformationResponseItem>,
}

pub async fn get_cities(State(state): State<AppState>) -> Result<Json<CitiesResponse>, StatusCode> {
    let cities = state
        .schedule
        .all_cities_with_extra_information()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(CitiesResponse {
        cities: cities
            .into_iter()
            .map(CityWithExtraInformationResponseItem::from)
            .collect(),
    }))
}

// --- destinations search ---

pub async fn get_destinations(
    State(state): State<AppState>,
    Query(DestinationsQueryParameters { from }): Query<DestinationsQueryParameters>,
) -> Result<Json<DestinationsResponse>, StatusCode> {
    let date = Utc::now().date_naive().format("%Y%m%d").to_string();

    let origin = CityId::from(from);
    let (destinations, cities) = state
        .schedule
        .find_destinations(&date, &origin)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = destinations
        .into_iter()
        .map(|d| DestinationResponseItem {
            station_id: d.destination(),
            duration: d.duration(),
            connections: d.number_of_connections(),
            visited_station_ids: d.intermediary_city_ids().iter().map(|s| **s).collect(),
        })
        .collect();

    let city_items = cities.into_iter().map(CityResponseItem::from).collect();

    Ok(Json(DestinationsResponse {
        destinations: items,
        cities: city_items,
    }))
}
