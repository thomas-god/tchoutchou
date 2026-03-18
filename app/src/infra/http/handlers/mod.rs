use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    app::schedule::{
        AddLabelToCityError, CityWithExtraInformation, LabelCreationError,
        RemoveLabelFromCityError, SetCityParentError,
    },
    domain::{City, CityId, CityLabel, CityLabelId},
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
pub struct CityLabelItem {
    id: i64,
    name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CityResponseItem {
    id: i64,
    name: String,
    country: String,
    lat: f64,
    lon: f64,
    parent: Option<i64>,
    labels: Vec<CityLabelItem>,
}

impl From<City> for CityResponseItem {
    fn from(city: City) -> Self {
        Self {
            id: **city.id(),
            name: city.name().to_string(),
            country: city.country().to_string(),
            lat: city.lat(),
            lon: city.lon(),
            parent: city.parent().map(|id| *id),
            labels: city
                .labels()
                .iter()
                .map(|label| CityLabelItem {
                    id: **label.id(),
                    name: label.name().to_string(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationsResponse {
    destinations: Vec<DestinationResponseItem>,
    cities: Vec<CityResponseItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelResponseItem {
    id: i64,
    name: String,
}

impl From<CityLabel> for LabelResponseItem {
    fn from(l: CityLabel) -> Self {
        Self {
            id: **l.id(),
            name: l.name().to_string(),
        }
    }
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
    labels: Vec<LabelResponseItem>,
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
            labels: c
                .city
                .labels()
                .iter()
                .cloned()
                .map(LabelResponseItem::from)
                .collect(),
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

// --- label management ---

#[derive(Deserialize)]
pub struct CreateLabelBody {
    name: String,
}

#[derive(Serialize)]
pub struct CreateLabelResponse {
    id: i64,
}

pub async fn create_label(
    State(state): State<AppState>,
    Json(body): Json<CreateLabelBody>,
) -> Result<(StatusCode, Json<CreateLabelResponse>), StatusCode> {
    match state.schedule.create_label(body.name.into()) {
        Ok(id) => Ok((StatusCode::CREATED, Json(CreateLabelResponse { id: *id }))),
        Err(LabelCreationError::LabelNameAlreadyExists) => Err(StatusCode::CONFLICT),
        Err(LabelCreationError::RepositoryError) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// --- list labels ---

#[derive(Debug, Clone, Serialize)]
pub struct LabelsResponse {
    labels: Vec<LabelResponseItem>,
}

pub async fn get_labels(State(state): State<AppState>) -> Result<Json<LabelsResponse>, StatusCode> {
    let labels = state
        .schedule
        .all_labels()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(LabelsResponse {
        labels: labels.into_iter().map(LabelResponseItem::from).collect(),
    }))
}

pub async fn add_label_to_city(
    State(state): State<AppState>,
    Path((city_id, label_id)): Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    match state
        .schedule
        .add_label_to_city(&CityId::from(city_id), &CityLabelId::from(label_id))
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(AddLabelToCityError::CityNotFound) | Err(AddLabelToCityError::LabelNotFound) => {
            Err(StatusCode::NOT_FOUND)
        }
        Err(AddLabelToCityError::RepositoryError) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn remove_label_from_city(
    State(state): State<AppState>,
    Path((city_id, label_id)): Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    match state
        .schedule
        .remove_label_from_city(&CityId::from(city_id), &CityLabelId::from(label_id))
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(RemoveLabelFromCityError::CityNotFound) => Err(StatusCode::NOT_FOUND),
        Err(RemoveLabelFromCityError::RepositoryError) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// --- set city parent ---

#[derive(Deserialize)]
pub struct SetCityParentBody {
    parent_id: Option<i64>,
}

pub async fn set_city_parent(
    State(state): State<AppState>,
    Path(city_id): Path<i64>,
    Json(body): Json<SetCityParentBody>,
) -> Result<StatusCode, StatusCode> {
    let parent = body.parent_id.map(CityId::from);
    match state
        .schedule
        .set_city_parent(&CityId::from(city_id), &parent)
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(SetCityParentError::CityNotFound) => Err(StatusCode::NOT_FOUND),
        Err(SetCityParentError::ParentCityNotFound) => Err(StatusCode::UNPROCESSABLE_ENTITY),
        Err(SetCityParentError::SameCity) => Err(StatusCode::UNPROCESSABLE_ENTITY),
        Err(SetCityParentError::RepositoryError) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
