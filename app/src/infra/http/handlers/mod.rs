use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    app::schedule::{
        ImportedStationId, ImportedStationRef, InternalStation, InternalStationId, MergeCandidate,
        RemapStationError, StationMergeCandidates,
    },
    domain::optim::{DestinationFilters, StationId, find_destinations},
    infra::http::AppState,
};

// --- list all stations ---

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StationItem {
    id: i64,
    name: String,
    lat: f64,
    lon: f64,
}

impl From<InternalStation> for StationItem {
    fn from(s: InternalStation) -> Self {
        Self {
            id: s.id().as_i64(),
            name: s.name().to_string(),
            lat: s.lat(),
            lon: s.lon(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AllStationsResponse {
    pub stations: Vec<StationItem>,
}

/// `GET /api/stations`
///
/// Returns every internal station with its id, name, latitude and longitude.
pub async fn get_all_stations(
    State(state): State<AppState>,
) -> Result<Json<AllStationsResponse>, StatusCode> {
    let Ok(stations) = state.schedule.list_all_stations() else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    Ok(Json(AllStationsResponse {
        stations: stations.into_iter().map(StationItem::from).collect(),
    }))
}

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
    max_connections: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationItem {
    station_id: i64,
    duration: usize,
    connections: usize,
    visited_station_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationsResponse {
    destinations: Vec<DestinationItem>,
}

pub async fn get_destinations(
    State(state): State<AppState>,
    Query(DestinationsQueryParameters {
        from,
        max_connections,
    }): Query<DestinationsQueryParameters>,
) -> Result<Json<DestinationsResponse>, StatusCode> {
    let date = Utc::now().date_naive().format("%Y%m%d").to_string();
    let graph = state
        .schedule
        .graph(&date)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let origin = StationId::from(from);
    let filters = DestinationFilters::new(max_connections.unwrap_or(1), 900, 3600 * 12);
    let destinations = find_destinations(&origin, &graph, &filters);

    let items = destinations
        .into_iter()
        .map(|d| DestinationItem {
            station_id: d.station_id(),
            duration: d.duration(),
            connections: d.connections_count(),
            visited_station_ids: d
                .intermediary_station_ids()
                .iter()
                .map(|s| s.as_i64())
                .collect(),
        })
        .collect();

    Ok(Json(DestinationsResponse {
        destinations: items,
    }))
}

// --- merge candidates ---

#[derive(Debug, Clone, Serialize)]
pub struct ImportedStationRefItem {
    source: String,
    source_id: String,
    name: String,
}

impl From<ImportedStationRef> for ImportedStationRefItem {
    fn from(r: ImportedStationRef) -> Self {
        Self {
            source: r.source,
            source_id: r.source_id.as_str().to_string(),
            name: r.name,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MergeCandidateItem {
    id: i64,
    name: String,
    lat: f64,
    lon: f64,
    distance_km: f64,
    sources: Vec<ImportedStationRefItem>,
}

impl From<MergeCandidate> for MergeCandidateItem {
    fn from(c: MergeCandidate) -> Self {
        Self {
            id: c.station().id().as_i64(),
            name: c.station().name().to_string(),
            lat: c.station().lat(),
            lon: c.station().lon(),
            distance_km: c.distance_km(),
            sources: c
                .station()
                .children()
                .iter()
                .cloned()
                .map(ImportedStationRefItem::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MergeCandidateGroup {
    id: i64,
    name: String,
    lat: f64,
    lon: f64,
    sources: Vec<ImportedStationRefItem>,
    candidates: Vec<MergeCandidateItem>,
}

impl From<StationMergeCandidates> for MergeCandidateGroup {
    fn from(g: StationMergeCandidates) -> Self {
        Self {
            id: g.station().id().as_i64(),
            name: g.station().name().to_string(),
            lat: g.station().lat(),
            lon: g.station().lon(),
            sources: g
                .station()
                .children()
                .iter()
                .cloned()
                .map(ImportedStationRefItem::from)
                .collect(),
            candidates: g
                .candidates()
                .iter()
                .cloned()
                .map(MergeCandidateItem::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MergeCandidatesResponse {
    pub stations: Vec<MergeCandidateGroup>,
}

/// `GET /api/stations/nearby?max_distance_km=<km>`
///
/// Returns every internal station that has at least one neighbour within
/// `max_distance_km` (default 1.0 km), each paired with its candidates sorted
/// by ascending distance. Stations with no nearby match are omitted.
pub async fn get_merge_candidates(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<MergeCandidatesResponse>, StatusCode> {
    let max_dist = params
        .get("max_distance_km")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.0);

    let Ok(groups) = state.schedule.find_all_merge_candidates(max_dist) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    Ok(Json(MergeCandidatesResponse {
        stations: groups.into_iter().map(MergeCandidateGroup::from).collect(),
    }))
}

// --- remap station ---

#[derive(Debug, Deserialize)]
pub struct RemapStationRequest {
    source: String,
    source_id: String,
    internal_id: i64,
}

/// `PATCH /api/stations/mapping`
///
/// Reassigns an imported station (identified by `source` + `source_id`) to a
/// different internal station (`internal_id`).
///
/// Returns `204 No Content` on success, `404 Not Found` when either the mapping
/// or the target internal station does not exist, and `500` on an internal error.
pub async fn remap_station(
    State(mut state): State<AppState>,
    Json(body): Json<RemapStationRequest>,
) -> StatusCode {
    let source_id = ImportedStationId::from(body.source_id);
    let internal_id = InternalStationId::from(body.internal_id);
    match state
        .schedule
        .remap_station(&body.source, &source_id, &internal_id)
    {
        Ok(()) => {
            tracing::info!(
                "Remap source station {} to internal station {}",
                source_id.as_str(),
                internal_id.as_i64()
            );
            StatusCode::NO_CONTENT
        }
        Err(RemapStationError::MappingNotFound | RemapStationError::InternalStationNotFound) => {
            StatusCode::NOT_FOUND
        }
        Err(RemapStationError::Error) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
