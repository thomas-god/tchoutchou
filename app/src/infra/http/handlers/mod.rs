use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::{
    app::schedule::InternalStation,
    domain::optim::{DestinationFilters, StationId, find_destinations},
    infra::http::AppState,
};

#[derive(Deserialize)]
pub struct QueryParameters {
    substring: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteStationResponseItem {
    name: String,
    id: i64,
}

impl From<InternalStation> for AutocompleteStationResponseItem {
    fn from(value: InternalStation) -> Self {
        Self {
            name: value.name().to_string(),
            id: value.id().as_i64(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteStationResponse {
    pub stations: Vec<AutocompleteStationResponseItem>,
}

pub async fn autocomplete_station(
    State(state): State<AppState>,
    Query(QueryParameters { substring }): Query<QueryParameters>,
) -> Result<Json<AutocompleteStationResponse>, StatusCode> {
    let Ok(stations) = state.schedule.search_stations_by_name(&substring, 10) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    Ok(Json(AutocompleteStationResponse {
        stations: stations
            .into_iter()
            .map(AutocompleteStationResponseItem::from)
            .collect(),
    }))
}

#[derive(Deserialize)]
pub struct DestinationsQueryParameters {
    from: i64,
    date: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationItem {
    station_id: i64,
    duration: usize,
    connections: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationsResponse {
    destinations: Vec<DestinationItem>,
}

pub async fn get_destinations(
    State(state): State<AppState>,
    Query(DestinationsQueryParameters { from, date }): Query<DestinationsQueryParameters>,
) -> Result<Json<DestinationsResponse>, StatusCode> {
    let graph = state
        .schedule
        .graph(&date)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let origin = StationId::from(from);
    let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

    let items = destinations
        .into_iter()
        .map(|d| DestinationItem {
            station_id: d.station_id(),
            duration: d.duration(),
            connections: d.connections_count(),
        })
        .collect();

    Ok(Json(DestinationsResponse {
        destinations: items,
    }))
}
