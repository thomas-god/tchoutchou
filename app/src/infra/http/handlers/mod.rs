use axum::{Json, http::StatusCode};
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteStationResponseItem {
    name: String,
    id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AutocompleteStationResponse {
    pub stations: Vec<AutocompleteStationResponseItem>,
}

pub async fn autocomplete_station() -> Result<Json<AutocompleteStationResponse>, StatusCode> {
    Ok(Json(AutocompleteStationResponse { stations: vec![] }))
}
