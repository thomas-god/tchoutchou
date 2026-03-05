use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use derive_more::{Constructor, From};

use crate::domain::optim::{Graph, StationId, Trip};

///////////////////////////////////////////////////////////////////////////////////////////////////
// `Imported*`` types describe the expected shapes of data to be ingested by the schedule service.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// A station represents a physical place where trains can depart from and arrive at.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct ImportedStation {
    id: ImportedStationId,
    name: String,
    lat: f64,
    lon: f64,
}

impl ImportedStation {
    pub fn id(&self) -> &ImportedStationId {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn lat(&self) -> f64 {
        self.lat
    }
    pub fn lon(&self) -> f64 {
        self.lon
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

impl ImportedStationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A trip leg is the core concept used by our domain. It represents a train leaving its *origin*
/// station at *departure* time and reaching its *destination* station at *arrival time*. For
/// example, a train leaving Paris Gare de Lyon at 09:20 and reaching Lyon Part-Dieu at 11:15.
///
/// The dates for which the train will actually run needs to be retrieved by the schedules
/// associated with the trips's [`ImportedRouteId`].
#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTripLeg {
    route: ImportedRouteId,
    origin: ImportedStationId,
    destination: ImportedStationId,
    departure: usize,
    arrival: usize,
}

impl ImportedTripLeg {
    pub fn origin(&self) -> &ImportedStationId {
        &self.origin
    }
    pub fn destination(&self) -> &ImportedStationId {
        &self.destination
    }
    pub fn route(&self) -> &ImportedRouteId {
        &self.route
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}

/// A schedule is a set of dates for which a particular trip/train will run.
#[derive(Debug, Clone, Constructor, PartialEq)]
pub struct ImportedSchedule {
    id: ImportedScheduleId,
    dates: Vec<String>,
}

impl ImportedSchedule {
    pub fn id(&self) -> &ImportedScheduleId {
        &self.id
    }
    pub fn dates(&self) -> &[String] {
        &self.dates
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedScheduleId(String);

impl ImportedScheduleId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A route is an abstract concept that group trips sharing some caracteristics (headsign,
/// schedule, etc.). For our domain, it's mostly used as an intermediary way to map a trip leg
/// to its schedule(s).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedRouteId(String);

impl ImportedRouteId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// [`TrainDataToImport`] represents the set of data requiered to ingest train schedules for a
/// particular source.
#[derive(Debug, Clone, Constructor)]
pub struct TrainDataToImport {
    stations: Vec<ImportedStation>,
    legs: Vec<ImportedTripLeg>,
    schedules: Vec<ImportedSchedule>,
    schedules_by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
    source: String,
}

impl TrainDataToImport {
    pub fn stations(&self) -> &[ImportedStation] {
        &self.stations
    }
    pub fn trip_legs(&self) -> &[ImportedTripLeg] {
        &self.legs
    }
    pub fn schedules(&self) -> &[ImportedSchedule] {
        &self.schedules
    }
    pub fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
        &self.schedules_by_route
    }
    pub fn source(&self) -> &str {
        &self.source
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// `Internal*` types describe internal, canonical, shapes of data, independant of the source,
// provider or input format.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// A stable, source-agnostic identifier for a physical station. An internal station aggregates one
/// or more [`ImportedStation`]s that represent the same physical place.
///
/// For example, *Paris Gare de l'Est* exists with distinct IDs in the SNCF and DB datasets, but will
/// point to the same [`InternalStationId`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, Ord)]
pub struct InternalStationId(i64);

impl InternalStationId {
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

impl From<i64> for InternalStationId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl From<&InternalStationId> for StationId {
    fn from(value: &InternalStationId) -> Self {
        StationId::from(value.as_i64())
    }
}

/// Canonical representation of a physical station, independent of any particular data source.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct InternalStation {
    id: InternalStationId,
    name: String,
    lat: f64,
    lon: f64,
}

impl InternalStation {
    pub fn id(&self) -> &InternalStationId {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn lat(&self) -> f64 {
        self.lat
    }
    pub fn lon(&self) -> f64 {
        self.lon
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct InternalTripLeg {
    origin: ImportedStationId,
    destination: ImportedStationId,
    departure: usize,
    arrival: usize,
}

impl InternalTripLeg {
    pub fn origin(&self) -> &ImportedStationId {
        &self.origin
    }
    pub fn destination(&self) -> &ImportedStationId {
        &self.destination
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// `Enriched*` are not directly exposed to the end-user, but are used to give admin-user context
// when deciding to update the mapping of [`ImportedStation`]s to [`InternalStation`]s.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// An [`InternalStation`] enriched with the imported stations mapped to it.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct EnrichedInternalStation {
    station: InternalStation,
    children: Vec<ImportedStationRef>,
}

impl EnrichedInternalStation {
    pub fn id(&self) -> &InternalStationId {
        self.station.id()
    }
    pub fn name(&self) -> &str {
        self.station.name()
    }
    pub fn lat(&self) -> f64 {
        self.station.lat()
    }
    pub fn lon(&self) -> f64 {
        self.station.lon()
    }
    pub fn children(&self) -> &[ImportedStationRef] {
        &self.children
    }
}

/// A reference to an imported station mapped to an [`InternalStation`], capturing the data source
/// and the original name as ingested. Useful as context when deciding whether two internal stations
/// should be merged.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct ImportedStationRef {
    pub source: String,
    pub source_id: ImportedStationId,
    pub name: String,
}

/// A candidate for merging with a given [`InternalStation`], together with the haversine distance
/// between them in kilometres.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct MergeCandidate {
    station: EnrichedInternalStation,
    distance_km: f64,
}

impl MergeCandidate {
    pub fn station(&self) -> &EnrichedInternalStation {
        &self.station
    }
    pub fn distance_km(&self) -> f64 {
        self.distance_km
    }
}

/// An [`InternalStation`] paired with all nearby stations that could represent the same physical
/// stop (merge candidates), sorted by ascending distance.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct StationMergeCandidates {
    station: EnrichedInternalStation,
    candidates: Vec<MergeCandidate>,
}

impl StationMergeCandidates {
    pub fn station(&self) -> &EnrichedInternalStation {
        &self.station
    }
    pub fn candidates(&self) -> &[MergeCandidate] {
        &self.candidates
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// `ScheduleService` related types.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Links an [`ImportedStationId`] to its canonical [`InternalStation`].
#[derive(Debug, Clone, PartialEq)]
pub struct StationMapping {
    pub source: String,
    pub source_id: ImportedStationId,
    pub internal_id: InternalStationId,
}

/// Describes a change to an [`ImportedStation`] detected during a timetable import.
#[derive(Debug, Clone, PartialEq)]
pub enum StationChange {
    /// The station did not exist in the repository before this import.
    Added(ImportedStation),
    /// The station existed but at least one attribute (name, lat, lon) changed.
    Updated(ImportedStation),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrainDataImportResult {
    pub station_changes: Vec<StationChange>,
    /// New internal stations that were automatically created because an
    /// incoming source station had no existing mapping.
    pub new_internal_stations: Vec<InternalStationId>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemapStationError {
    /// No mapping exists for the given `(source, station_id)` pair.
    MappingNotFound,
    /// The target [`InternalStationId`] does not exist in the repository.
    InternalStationNotFound,
    Error,
}

/// Persistence contract for stations, trips and schedules.
pub trait TrainDataRepository {
    /// Atomically replace all timetable data (trips, schedules, route–schedule mappings)
    /// and upsert stations, returning information about which stations are new or changed.
    /// For each incoming source station that has no existing mapping to an internal
    /// station, a new [`InternalStation`] is created and linked automatically.
    fn import_timetable(&mut self, data: TrainDataToImport) -> TrainDataImportResult;

    /// Return only the trips whose route runs on `date` (format `YYYYMMDD`).
    /// Filtering is done at the persistence layer for efficiency.
    fn trips_for_date(&self, date: &str) -> Vec<InternalTripLeg>;

    /// Return all source-to-internal station mappings.
    fn station_mappings(&self) -> Vec<StationMapping>;

    /// Reassign an existing source station mapping to a different internal station.
    ///
    /// Returns [`RemapError::MappingNotFound`] when no mapping exists for
    /// `(source, source_id)`, and [`RemapError::InternalStationNotFound`] when
    /// `new_internal_id` does not refer to a known internal station.
    fn update_station_mapping(
        &mut self,
        source: &str,
        source_id: &ImportedStationId,
        new_internal_id: &InternalStationId,
    ) -> Result<(), RemapStationError>;

    /// Return up to `limit` internal stations whose name contains `query`
    /// (case-insensitive), ordered alphabetically. Intended for autocomplete.
    fn search_internal_stations_by_name(&self, query: &str, limit: usize) -> Vec<InternalStation>;

    /// Return all internal stations, each enriched with the imported station(s) mapped to it.
    fn all_internal_stations_enriched(&self) -> Vec<EnrichedInternalStation>;
}

/// Application service that aggregates data from various importers, persists it through a
/// [`TrainDataRepository`], and exposes a [`Graph`] ready for the optimisation algorithms in
/// [`crate::domain::optim`].
pub struct ScheduleService<R: TrainDataRepository> {
    repository: Arc<Mutex<R>>,
    /// Keyed by date string (`YYYYMMDD`). Values are reference-counted so callers receive a
    /// cheap handle (`Arc<Graph>`) without deep-cloning the graph. Invalidated on ingest.
    graph_cache: Arc<Mutex<HashMap<String, Arc<Graph>>>>,
}

impl<R: TrainDataRepository> Clone for ScheduleService<R> {
    fn clone(&self) -> Self {
        Self {
            repository: self.repository.clone(),
            graph_cache: self.graph_cache.clone(),
        }
    }
}

impl<R: TrainDataRepository> ScheduleService<R> {
    pub fn new(repository: R) -> Self {
        Self {
            repository: Arc::new(Mutex::new(repository)),
            graph_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ingest(&mut self, data: TrainDataToImport) -> Result<TrainDataImportResult, ()> {
        let result = self
            .repository
            .lock()
            .map_err(|_| ())
            .map(|mut repo| repo.import_timetable(data))?;
        self.graph_cache.lock().map_err(|_| ())?.clear();
        Ok(result)
    }

    /// Reassign a [`ImportedStation`] to a different [`InternalStation`].
    ///
    /// Any now-orphaned [`InternalStation`] will be deleted in the process.
    pub fn remap_station(
        &mut self,
        source: &str,
        source_id: &ImportedStationId,
        new_internal_id: &InternalStationId,
    ) -> Result<(), RemapStationError> {
        self.repository
            .lock()
            .map_err(|_| RemapStationError::Error)
            .and_then(|mut repo| repo.update_station_mapping(source, source_id, new_internal_id))
    }

    /// Return up to `limit` [`InternalStation`]s whose name contains `query` (case-insensitive),
    /// ordered alphabetically. Intended for autocomplete.
    pub fn search_stations_by_name(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<InternalStation>, ()> {
        self.repository
            .lock()
            .map_err(|_| ())
            .map(|repo| repo.search_internal_stations_by_name(query, limit))
    }

    pub fn list_all_stations(&self) -> Result<Vec<InternalStation>, ()> {
        Ok(self
            .repository
            .lock()
            .map_err(|_| ())?
            .all_internal_stations_enriched()
            .into_iter()
            .map(|e| InternalStation::new(e.id().clone(), e.name().to_string(), e.lat(), e.lon()))
            .collect())
    }

    /// Return all [`InternalStation`]s that have at least one neighbour within `max_distance_km`
    /// (haversine), each paired with its sorted candidate list. Stations with no nearby match
    /// are omitted. Intended for bulk merge-candidate discovery.
    ///
    /// Only stations from **different** dataset sources are considered as candidates for each
    /// other. Stations whose source sets overlap are not in scope (duplicates within a single
    /// dataset are a data-quality issue for that dataset, not something we reconcile here).
    pub fn find_all_merge_candidates(
        &self,
        max_distance_km: f64,
    ) -> Result<Vec<StationMergeCandidates>, ()> {
        let all = self
            .repository
            .lock()
            .map_err(|_| ())?
            .all_internal_stations_enriched();

        Ok(all
            .iter()
            .filter_map(|station| {
                let mut candidates: Vec<MergeCandidate> = all
                    .iter()
                    .filter(|other| other.id() != station.id())
                    .filter(|other| has_disjoint_sources(station, other))
                    .filter_map(|other| {
                        let d =
                            haversine_km(station.lat(), station.lon(), other.lat(), other.lon());
                        if d <= max_distance_km {
                            Some(MergeCandidate::new(other.clone(), d))
                        } else {
                            None
                        }
                    })
                    .collect();

                if candidates.is_empty() {
                    return None;
                }

                candidates.sort_by(|a, b| {
                    a.distance_km()
                        .partial_cmp(&b.distance_km())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                Some(StationMergeCandidates::new(station.clone(), candidates))
            })
            .collect())
    }

    /// Build a [`Graph`] from trips active on `date` (format `YYYYMMDD`).
    ///
    /// [`ImportedStation`]s are resolved to their canonical [`InternalStationId`] so that
    /// connections to the same physical station are shared across providers.
    ///
    /// The result is an [`Arc`] into the internal cache. Repeated calls for the same date return
    /// a new handle to the same allocation — no graph data is ever copied.
    pub fn graph(&self, date: &str) -> Result<Arc<Graph>, ()> {
        // Fast path: return a cached handle without touching the repository.
        {
            let cache = self.graph_cache.lock().map_err(|_| ())?;
            if let Some(graph) = cache.get(date) {
                return Ok(Arc::clone(graph));
            }
        }

        // Cache miss: load from the repository, then populate the cache.
        let start = Instant::now();
        let graph = {
            let repo = self.repository.lock().map_err(|_| ())?;
            let trips = repo.trips_for_date(date);
            let mappings = repo.station_mappings();
            tracing::info!(
                duration = format!("{:?}", start.elapsed()),
                date,
                "Graph loaded"
            );
            Arc::new(build_graph(&trips, &mappings))
        };

        self.graph_cache
            .lock()
            .map_err(|_| ())?
            .insert(date.to_owned(), Arc::clone(&graph));

        Ok(graph)
    }
}

/// Map imported data to domain types.
///
/// [`ImportedStation`]s are resolved through `mappings` to their canonical [`InternalStationId`],
/// so that two providers whose stations share the same internal station are connected in the
/// resulting graph. Each [`StationId`] directly mirrors the corresponding [`InternalStationId`]
/// value.
fn build_graph(trips: &[InternalTripLeg], mappings: &[StationMapping]) -> Graph {
    // 1. ImportedStationId → InternalStationId from station mappings.
    let imported_to_internal: HashMap<&ImportedStationId, &InternalStationId> = mappings
        .iter()
        .map(|m| (&m.source_id, &m.internal_id))
        .collect();

    // 2. Build the graph.
    let mut trips_by_nodes: HashMap<StationId, Vec<Trip>> = HashMap::new();

    for trip in trips {
        let Some(&origin_internal) = imported_to_internal.get(trip.origin()) else {
            continue;
        };
        let Some(&destination_internal) = imported_to_internal.get(trip.destination()) else {
            continue;
        };
        let origin = StationId::from(origin_internal);
        let destination = StationId::from(destination_internal);
        let domain_trip = Trip::new(origin, destination, trip.departure(), trip.arrival());
        trips_by_nodes.entry(origin).or_default().push(domain_trip);
    }

    Graph::new(trips_by_nodes)
}

/// Returns `true` when the two stations have no data source in common, meaning they could
/// represent the same physical place imported from different providers. Stations that share
/// at least one source are considered intra-dataset duplicates and are not merge candidates.
fn has_disjoint_sources(a: &EnrichedInternalStation, b: &EnrichedInternalStation) -> bool {
    let sources_a: std::collections::HashSet<&str> =
        a.children().iter().map(|c| c.source.as_str()).collect();
    let sources_b: std::collections::HashSet<&str> =
        b.children().iter().map(|c| c.source.as_str()).collect();
    sources_a.is_disjoint(&sources_b)
}

/// Haversine great-circle distance in kilometres between two (lat, lon) points in decimal degrees.
fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[cfg(test)]
pub mod test_utils {
    use mockall::mock;

    use super::*;

    mock! {
        pub TrainDataRepository {}

        impl Clone for TrainDataRepository {
            fn clone(&self) -> Self;
        }

        impl TrainDataRepository for TrainDataRepository {
            fn import_timetable(&mut self, data: TrainDataToImport) -> TrainDataImportResult;
            fn trips_for_date(&self, date: &str) -> Vec<InternalTripLeg>;
            fn station_mappings(&self) -> Vec<StationMapping>;
            fn update_station_mapping(
                &mut self,
                source: &str,
                source_id: &ImportedStationId,
                new_internal_id: &InternalStationId,
            ) -> Result<(), RemapStationError>;
            fn search_internal_stations_by_name(&self, query: &str, limit: usize) -> Vec<InternalStation>;
            fn all_internal_stations_enriched(&self) -> Vec<EnrichedInternalStation>;
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::app::schedule::test_utils::MockTrainDataRepository;

    use super::*;

    const TEST_DATE: &str = "20260303";

    // -- helpers --

    fn station(id: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            0.0,
            0.0,
        )
    }

    fn sid(id: &str) -> ImportedStationId {
        ImportedStationId::from(id.to_owned())
    }

    fn empty_result() -> TrainDataImportResult {
        TrainDataImportResult {
            station_changes: vec![],
            new_internal_stations: vec![],
        }
    }

    fn smapping(source_id: &str, internal_id: i64) -> StationMapping {
        StationMapping {
            source: "source".to_owned(),
            source_id: ImportedStationId::from(source_id.to_owned()),
            internal_id: InternalStationId::from(internal_id),
        }
    }

    fn make_importer(source: &str, station_ids: &[&str]) -> TrainDataToImport {
        TrainDataToImport::new(
            station_ids.iter().map(|id| station(id)).collect(),
            vec![],
            vec![],
            HashMap::new(),
            source.to_owned(),
        )
    }

    // ---- graph building ----

    #[test]
    fn ingest_builds_graph() {
        // trips_for_date returns already-filtered trips
        let trips = vec![InternalTripLeg::new(sid("A"), sid("B"), 100, 200)];
        let mappings = vec![smapping("A", 1), smapping("B", 2)];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_station_mappings().return_const(mappings);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 1);
    }

    #[test]
    fn empty_repository_produces_empty_graph() {
        let mut mock = MockTrainDataRepository::new();
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(vec![]);
        mock.expect_station_mappings().return_const(vec![]);

        let service = ScheduleService::new(mock);
        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_origin_is_skipped() {
        // "X" has no station mapping; the trip is active but unmappable
        let trips = vec![InternalTripLeg::new(sid("X"), sid("B"), 100, 200)];
        let mappings = vec![smapping("A", 1), smapping("B", 2)];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_station_mappings().return_const(mappings);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 0);
        assert_eq!(graph.trips_from(StationId::from(2)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_destination_is_skipped() {
        // "X" has no station mapping; the trip is active but unmappable
        let trips = vec![InternalTripLeg::new(sid("A"), sid("X"), 100, 200)];
        let mappings = vec![smapping("A", 1), smapping("B", 2)];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_station_mappings().return_const(mappings);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 0);
    }

    #[test]
    fn multiple_trips_from_same_origin_are_all_indexed() {
        let trips = vec![
            InternalTripLeg::new(sid("A"), sid("B"), 100, 200),
            InternalTripLeg::new(sid("A"), sid("C"), 300, 400),
            InternalTripLeg::new(sid("A"), sid("B"), 500, 600),
        ];
        // A→internal(1)=StationId(1), B→internal(2)=StationId(2), C→internal(3)=StationId(3)
        let mappings = vec![smapping("A", 1), smapping("B", 2), smapping("C", 3)];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_station_mappings().return_const(mappings);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B", "C"]));

        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 3);
    }

    #[test]
    fn shared_internal_station_connects_providers() {
        // Station "A-db" from provider DB and "A-sncf" from provider SNCF
        // both map to the same internal station (id=1).
        // trips_for_date already returns only trips active on TEST_DATE
        let trips = vec![
            InternalTripLeg::new(sid("A-db"), sid("B"), 100, 200),
            InternalTripLeg::new(sid("A-sncf"), sid("B"), 300, 400),
        ];
        // A-db and A-sncf share internal station 1; B is internal station 2.
        let mappings = vec![
            StationMapping {
                source: "db".to_owned(),
                source_id: sid("A-db"),
                internal_id: InternalStationId::from(1_i64),
            },
            StationMapping {
                source: "sncf".to_owned(),
                source_id: sid("A-sncf"),
                internal_id: InternalStationId::from(1_i64),
            },
            smapping("B", 2),
        ];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_station_mappings().return_const(mappings);

        let service = ScheduleService::new(mock);
        let graph = service.graph(TEST_DATE).expect("should build graph");
        // A-* maps to internal station 1 → StationId(1); B maps to internal station 2 → StationId(2).
        // Both trips depart from StationId(1).
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 2);
        assert_eq!(graph.trips_from(StationId::from(2)).len(), 0);
    }

    // ---- remap_station ----

    #[test]
    fn remap_station_passes_through_success() {
        let src = sid("A");
        let internal_id = InternalStationId::from(1_i64);

        let mut mock = MockTrainDataRepository::new();
        mock.expect_update_station_mapping()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let mut service = ScheduleService::new(mock);
        assert_eq!(service.remap_station("db", &src, &internal_id), Ok(()));
    }

    #[test]
    fn remap_station_propagates_mapping_not_found() {
        let src = sid("ghost");
        let internal_id = InternalStationId::from(1_i64);

        let mut mock = MockTrainDataRepository::new();
        mock.expect_update_station_mapping()
            .times(1)
            .returning(|_, _, _| Err(RemapStationError::MappingNotFound));

        let mut service = ScheduleService::new(mock);
        assert_eq!(
            service.remap_station("db", &src, &internal_id),
            Err(RemapStationError::MappingNotFound)
        );
    }

    #[test]
    fn remap_station_propagates_internal_station_not_found() {
        let src = sid("A");
        let ghost_internal = InternalStationId::from(99999_i64);

        let mut mock = MockTrainDataRepository::new();
        mock.expect_update_station_mapping()
            .times(1)
            .returning(|_, _, _| Err(RemapStationError::InternalStationNotFound));

        let mut service = ScheduleService::new(mock);
        assert_eq!(
            service.remap_station("db", &src, &ghost_internal),
            Err(RemapStationError::InternalStationNotFound)
        );
    }

    // ---- search_stations_by_name ----

    fn internal_station(id: i64, name: &str) -> InternalStation {
        InternalStation::new(InternalStationId::from(id), name.to_owned(), 0.0, 0.0)
    }

    #[test]
    fn search_stations_by_name_delegates_to_repository() {
        let expected = vec![
            internal_station(1, "Paris Gare de Lyon"),
            internal_station(2, "Paris Nord"),
        ];
        let expected_clone = expected.clone();

        let mut mock = MockTrainDataRepository::new();
        mock.expect_search_internal_stations_by_name()
            .withf(|q, lim| q == "paris" && *lim == 10)
            .times(1)
            .return_once(move |_, _| expected_clone);

        let service = ScheduleService::new(mock);
        assert_eq!(
            service.search_stations_by_name("paris", 10).unwrap(),
            expected
        );
    }

    // ---- find_all_merge_candidates ----

    fn enriched(id: i64, name: &str, lat: f64, lon: f64) -> EnrichedInternalStation {
        EnrichedInternalStation::new(
            InternalStation::new(InternalStationId::from(id), name.to_owned(), lat, lon),
            vec![],
        )
    }

    fn enriched_with_child(
        id: i64,
        name: &str,
        lat: f64,
        lon: f64,
        source: &str,
        source_id: &str,
    ) -> EnrichedInternalStation {
        EnrichedInternalStation::new(
            InternalStation::new(InternalStationId::from(id), name.to_owned(), lat, lon),
            vec![ImportedStationRef::new(
                source.to_owned(),
                ImportedStationId::from(source_id.to_owned()),
                name.to_owned(),
            )],
        )
    }

    #[test]
    fn merge_candidates_empty_stations_returns_empty() {
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| vec![]);

        let service = ScheduleService::new(mock);
        assert!(service.find_all_merge_candidates(100.0).unwrap().is_empty());
    }

    #[test]
    fn merge_candidates_no_pair_within_threshold_returns_empty() {
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| vec![enriched(1, "A", 0.0, 0.0), enriched(2, "B", 10.0, 10.0)]);

        let service = ScheduleService::new(mock);
        assert!(service.find_all_merge_candidates(1.0).unwrap().is_empty());
    }

    #[test]
    fn merge_candidates_close_pair_appears_as_mutual_candidates() {
        // At the equator, 0.001° lon ≈ 0.11 km.
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| vec![enriched(1, "A", 0.0, 0.0), enriched(2, "B", 0.0, 0.001)]);

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(1.0).unwrap();

        let names: Vec<&str> = result.iter().map(|g| g.station().name()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"A"));
        assert!(names.contains(&"B"));

        let group_a = result.iter().find(|g| g.station().name() == "A").unwrap();
        assert_eq!(group_a.candidates().len(), 1);
        assert_eq!(group_a.candidates()[0].station().name(), "B");
    }

    #[test]
    fn merge_candidates_sorted_by_distance_ascending() {
        // At the equator: B ≈ 0.11 km, C ≈ 0.56 km from A.
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| {
                vec![
                    enriched(1, "A", 0.0, 0.0),
                    enriched(2, "B", 0.0, 0.001),
                    enriched(3, "C", 0.0, 0.005),
                ]
            });

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(10.0).unwrap();
        let group_a = result.iter().find(|g| g.station().name() == "A").unwrap();
        let cand_names: Vec<&str> = group_a
            .candidates()
            .iter()
            .map(|c| c.station().name())
            .collect();
        assert_eq!(
            cand_names,
            ["B", "C"],
            "candidates must be sorted ascending"
        );
    }

    #[test]
    fn merge_candidates_same_source_stations_are_excluded() {
        // Two close stations that both come from the same source must NOT be candidates.
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| {
                vec![
                    enriched_with_child(1, "A", 0.0, 0.0, "sncf", "sncf-a"),
                    enriched_with_child(2, "B", 0.0, 0.001, "sncf", "sncf-b"),
                ]
            });

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(1.0).unwrap();
        assert!(
            result.is_empty(),
            "intra-dataset duplicates must not appear as merge candidates"
        );
    }

    #[test]
    fn merge_candidates_cross_source_stations_are_included() {
        // Two close stations from different sources must be candidates for each other.
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| {
                vec![
                    enriched_with_child(1, "A", 0.0, 0.0, "sncf", "sncf-a"),
                    enriched_with_child(2, "B", 0.0, 0.001, "db", "db-b"),
                ]
            });

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(1.0).unwrap();
        assert_eq!(result.len(), 2, "both stations must appear in the result");
        let group_a = result.iter().find(|g| g.station().name() == "A").unwrap();
        assert_eq!(group_a.candidates().len(), 1);
        assert_eq!(group_a.candidates()[0].station().name(), "B");
    }

    #[test]
    fn merge_candidates_shared_source_station_excluded_mixed_set() {
        // A (sncf) is close to B (db) and to C (sncf).
        // B is a cross-source candidate for A; C shares the source with A and must be excluded.
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| {
                vec![
                    enriched_with_child(1, "A", 0.0, 0.0, "sncf", "sncf-a"),
                    enriched_with_child(2, "B", 0.0, 0.001, "db", "db-b"),
                    enriched_with_child(3, "C", 0.0, 0.002, "sncf", "sncf-c"),
                ]
            });

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(10.0).unwrap();

        let group_a = result.iter().find(|g| g.station().name() == "A").unwrap();
        let cand_names: Vec<&str> = group_a
            .candidates()
            .iter()
            .map(|c| c.station().name())
            .collect();
        assert_eq!(
            cand_names,
            ["B"],
            "only the cross-source station B should be a candidate for A"
        );
    }

    #[test]
    fn merge_candidates_children_forwarded_from_enriched_station() {
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_internal_stations_enriched()
            .return_once(|| {
                vec![
                    enriched_with_child(1, "A", 0.0, 0.0, "sncf", "sncf-a"),
                    enriched_with_child(2, "B", 0.0, 0.001, "db", "db-b"),
                ]
            });

        let service = ScheduleService::new(mock);
        let result = service.find_all_merge_candidates(1.0).unwrap();

        let group_a = result.iter().find(|g| g.station().name() == "A").unwrap();
        assert_eq!(group_a.station().children().len(), 1);
        assert_eq!(group_a.station().children()[0].source, "sncf");

        let cand = &group_a.candidates()[0];
        assert_eq!(cand.station().name(), "B");
        assert_eq!(cand.station().children().len(), 1);
        assert_eq!(cand.station().children()[0].source, "db");
    }

    // ---- graph cache ----

    #[test]
    fn graph_hits_repository_only_once_for_same_date() {
        let trips = vec![InternalTripLeg::new(sid("A"), sid("B"), 100, 200)];
        let mappings = vec![smapping("A", 1), smapping("B", 2)];

        let mut mock = MockTrainDataRepository::new();
        // trips_for_date and station_mappings must each be called exactly once despite
        // two graph() calls for the same date.
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .times(1)
            .return_const(trips);
        mock.expect_station_mappings()
            .times(1)
            .return_const(mappings);

        let service = ScheduleService::new(mock);
        let g1 = service.graph(TEST_DATE).expect("first call");
        let g2 = service.graph(TEST_DATE).expect("second call (cached)");
        assert_eq!(
            g1.trips_from(StationId::from(1)).len(),
            g2.trips_from(StationId::from(1)).len()
        );
    }

    #[test]
    fn ingest_invalidates_graph_cache() {
        let trips_before = vec![InternalTripLeg::new(sid("A"), sid("B"), 100, 200)];
        let trips_after = vec![];
        let mappings = vec![smapping("A", 1), smapping("B", 2)];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        // trips_for_date is called twice: once before ingest (cache miss) and once
        // after ingest (cache invalidated, so another miss).
        mock.expect_trips_for_date()
            .withf(|d| d == TEST_DATE)
            .times(2)
            .returning({
                let mut call = 0usize;
                move |_| {
                    call += 1;
                    if call == 1 {
                        trips_before.clone()
                    } else {
                        trips_after.clone()
                    }
                }
            });
        mock.expect_station_mappings()
            .times(2)
            .return_const(mappings);

        let mut service = ScheduleService::new(mock);

        let g_before = service.graph(TEST_DATE).expect("before ingest");
        assert_eq!(g_before.trips_from(StationId::from(1)).len(), 1);

        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let g_after = service.graph(TEST_DATE).expect("after ingest");
        assert_eq!(g_after.trips_from(StationId::from(1)).len(), 0);
    }
}
