use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::{app::schedule::InternalStation, infra::http::AppState};

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
