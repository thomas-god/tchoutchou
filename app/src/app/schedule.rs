use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use derive_more::{Constructor, From};

use crate::domain::optim::{Graph, StationId, Trip};

///////////////////////////////////////////////////////////////////////////////////////////////////
/// `Imported*`` types describe the expected shapes of data to be ingested by the schedule service.
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
/// `Internal*` types describe internal, canonical, shapes of data, independant of the source,
/// provider or input format.
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

///////////////////////////////////////////////////////////////////////////////////////////////////
/// `ScheduleService` related types.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Links an [`ImportedStationId`] to its canonical [`InternalStation`].
#[derive(Debug, Clone, PartialEq)]
pub struct StationMapping {
    pub source: String,
    pub source_id: ImportedStationId,
    pub internal_id: InternalStationId,
}

/// Describes a change to a station detected during a timetable import.
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
    fn all_stations(&self) -> Vec<ImportedStation>;
    fn all_schedules(&self) -> Vec<ImportedSchedule>;
    fn all_trips(&self) -> Vec<ImportedTripLeg>;
    fn schedules_by_route(&self) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>>;
    /// Return all canonical internal stations.
    fn internal_stations(&self) -> Vec<InternalStation>;
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
    /// (case-insensitive), ordered alphabetically.  Intended for autocomplete.
    fn search_internal_stations_by_name(&self, query: &str, limit: usize) -> Vec<InternalStation>;
}

/// Application service that aggregates data from various importers, persists it through a
/// [`TrainDataRepository`], and exposes a [`Graph`] ready for the optimisation algorithms in
/// [`crate::domain::optim`].
pub struct ScheduleService<R: TrainDataRepository> {
    repository: Arc<Mutex<R>>,
}

impl<R: TrainDataRepository> Clone for ScheduleService<R> {
    fn clone(&self) -> Self {
        Self {
            repository: self.repository.clone(),
        }
    }
}

impl<R: TrainDataRepository> ScheduleService<R> {
    pub fn new(repository: R) -> Self {
        Self {
            repository: Arc::new(Mutex::new(repository)),
        }
    }

    pub fn ingest(&mut self, data: TrainDataToImport) -> Result<TrainDataImportResult, ()> {
        self.repository
            .lock()
            .map_err(|_| ())
            .map(|mut repo| repo.import_timetable(data))
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

    /// Build a [`Graph`] from everything currently held by the repository.
    pub fn graph(&self) -> Result<Graph, ()> {
        let repo = self.repository.lock().map_err(|_| ())?;
        let stations = repo.all_stations();
        let trips = repo.all_trips();
        Ok(build_graph(&stations, &trips))
    }
}

/// Map imported data to domain types.
///
/// `ImportedStationId` strings are mapped to compact [`StationId`] integers by
/// enumeration order so that the graph stays independent from the raw string ids.
fn build_graph(stations: &[ImportedStation], trips: &[ImportedTripLeg]) -> Graph {
    let id_map: HashMap<&ImportedStationId, StationId> = stations
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id(), StationId::from(i)))
        .collect();

    let mut trips_by_nodes: HashMap<StationId, Vec<Trip>> = HashMap::new();

    for trip in trips {
        let Some(&origin) = id_map.get(trip.origin()) else {
            continue;
        };
        let Some(&destination) = id_map.get(trip.destination()) else {
            continue;
        };
        let domain_trip = Trip::new(origin, destination, trip.departure(), trip.arrival());
        trips_by_nodes.entry(origin).or_default().push(domain_trip);
    }

    Graph::new(trips_by_nodes)
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
            fn all_stations(&self) -> Vec<ImportedStation>;
            fn all_schedules(&self) -> Vec<ImportedSchedule>;
            fn all_trips(&self) -> Vec<ImportedTripLeg>;
            fn schedules_by_route(&self) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>>;
            /// Return all canonical internal stations.
            fn internal_stations(&self) -> Vec<InternalStation>;
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
            fn search_internal_stations_by_name(&self, query: &str, limit: usize) -> Vec<InternalStation>;
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::app::schedule::test_utils::MockTrainDataRepository;

    use super::*;

    // -- helpers --

    fn station(id: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            0.0,
            0.0,
        )
    }

    fn route(id: &str) -> ImportedRouteId {
        ImportedRouteId::from(id.to_owned())
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
        let stations = vec![station("A"), station("B")];
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("A"),
            sid("B"),
            100,
            200,
        )];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_all_stations().return_const(stations);
        mock.expect_all_trips().return_const(trips);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph().expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 1);
    }

    #[test]
    fn empty_repository_produces_empty_graph() {
        let mut mock = MockTrainDataRepository::new();
        mock.expect_all_stations().return_const(vec![]);
        mock.expect_all_trips().return_const(vec![]);

        let service = ScheduleService::new(mock);
        let graph = service.graph().expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_origin_is_skipped() {
        let stations = vec![station("A"), station("B")];
        // "X" is not in the station list
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("X"),
            sid("B"),
            100,
            200,
        )];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_all_stations().return_const(stations);
        mock.expect_all_trips().return_const(trips);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph().expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_destination_is_skipped() {
        let stations = vec![station("A"), station("B")];
        // "X" is not in the station list
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("A"),
            sid("X"),
            100,
            200,
        )];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_all_stations().return_const(stations);
        mock.expect_all_trips().return_const(trips);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph().expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn multiple_trips_from_same_origin_are_all_indexed() {
        let stations = vec![station("A"), station("B"), station("C")];
        let trips = vec![
            ImportedTripLeg::new(route("R1"), sid("A"), sid("B"), 100, 200),
            ImportedTripLeg::new(route("R1"), sid("A"), sid("C"), 300, 400),
            ImportedTripLeg::new(route("R1"), sid("A"), sid("B"), 500, 600),
        ];

        let mut mock = MockTrainDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_all_stations().return_const(stations);
        mock.expect_all_trips().return_const(trips);

        let mut service = ScheduleService::new(mock);
        let _ = service.ingest(make_importer("source", &["A", "B", "C"]));

        let graph = service.graph().expect("should build graph");
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 3);
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
}
