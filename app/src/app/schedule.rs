use std::collections::HashMap;

use derive_more::{Constructor, From};

use crate::domain::optim::{Graph, StationId, Trip};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

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

impl ImportedStationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedScheduleId(String);

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

impl ImportedScheduleId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A route is an abstract concept that group trip that share some caracteristics (headsign,
/// schedule, etc.). For our domain, it's mostly used as an intermediary way to map a trip leg
/// to its schedules.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedRouteId(String);

impl ImportedRouteId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A trip leg is the core concept used by our domain. It represents a train leaving its origin
/// station at departure and reaching its destination at arrival. The actual dates the train will
/// actually run needs to be retrieve by the schedules associated with its [`ImportedRouteId`].
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

pub trait ImportTrainData {
    fn stations(&self) -> &[ImportedStation];
    fn trip_legs(&self) -> &[ImportedTripLeg];
    fn schedules(&self) -> &[ImportedSchedule];
    fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>>;
    fn source(&self) -> &str;
}

/// A stable, source-agnostic identifier for a physical station.  An internal
/// station aggregates one or more raw (source) stations that represent the
/// same physical place.
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

/// Canonical representation of a physical station, independent of any
/// particular data source.
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

/// Links a single raw (source) station to its canonical [`InternalStation`].
#[derive(Debug, Clone, PartialEq)]
pub struct StationMapping {
    /// The data source the raw station belongs to (e.g. `"db"` or `"sncf"`).
    pub source: String,
    /// The identifier used by that source for this station.
    pub source_id: ImportedStationId,
    /// The canonical internal station this source station maps to.
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

/// Outcome returned by [`TrainDataRepository::import_timetable`].
#[derive(Debug, Clone, PartialEq)]
pub struct TimetableImportResult {
    /// Stations that are new or whose attributes changed in this import.
    pub station_changes: Vec<StationChange>,
    /// New internal stations that were automatically created because an
    /// incoming source station had no existing mapping.
    pub new_internal_stations: Vec<InternalStationId>,
}

/// Persistence contract for stations, trips and schedules.
pub trait TrainDataRepository {
    /// Atomically replace all timetable data (trips, schedules, route–schedule mappings)
    /// and upsert stations, returning information about which stations are new or changed.
    /// For each incoming source station that has no existing mapping to an internal
    /// station, a new [`InternalStation`] is created and linked automatically.
    fn import_timetable<D: ImportTrainData>(&mut self, data: &D) -> TimetableImportResult;
    fn all_stations(&self) -> Vec<ImportedStation>;
    fn all_schedules(&self) -> Vec<ImportedSchedule>;
    fn all_trips(&self) -> Vec<ImportedTripLeg>;
    fn schedules_by_route(&self) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>>;
    /// Return all canonical internal stations.
    fn internal_stations(&self) -> Vec<InternalStation>;
    /// Return all source-to-internal station mappings.
    fn station_mappings(&self) -> Vec<StationMapping>;
}

/// Application service that aggregates data from various importers, persists it
/// through a [`StationAndTripRepository`], and exposes a [`Graph`] ready for the
/// optimisation algorithms in [`crate::domain::optim`].
pub struct ScheduleService<R: TrainDataRepository> {
    repository: R,
}

impl<R: TrainDataRepository> ScheduleService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    /// Feed stations, schedules and trips from any importer into the repository.
    /// Returns a [`TimetableImportResult`] describing which stations are new or changed.
    pub fn ingest(&mut self, importer: &impl ImportTrainData) -> TimetableImportResult {
        self.repository.import_timetable(importer)
    }

    /// Build a [`Graph`] from everything currently held by the repository.
    pub fn graph(&self) -> Graph {
        let stations = self.repository.all_stations();
        let trips = self.repository.all_trips();
        build_graph(&stations, &trips)
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
mod tests {

    use super::*;

    struct InMemoryTestRepository {
        stations: Vec<ImportedStation>,
        schedules: Vec<ImportedSchedule>,
        trips: Vec<ImportedTripLeg>,
        schedules_by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
        internal_stations: Vec<InternalStation>,
        station_mappings: Vec<StationMapping>,
        next_internal_id: i64,
    }

    impl InMemoryTestRepository {
        fn empty() -> Self {
            Self {
                stations: vec![],
                schedules: vec![],
                trips: vec![],
                schedules_by_route: HashMap::new(),
                internal_stations: vec![],
                station_mappings: vec![],
                next_internal_id: 1,
            }
        }
    }

    impl TrainDataRepository for InMemoryTestRepository {
        fn import_timetable<D: ImportTrainData>(&mut self, data: &D) -> TimetableImportResult {
            let existing: HashMap<&ImportedStationId, &ImportedStation> =
                self.stations.iter().map(|s| (s.id(), s)).collect();

            let mut station_changes = Vec::new();
            for s in data.stations() {
                match existing.get(s.id()) {
                    None => station_changes.push(StationChange::Added(s.clone())),
                    Some(&old) if old != s => {
                        station_changes.push(StationChange::Updated(s.clone()))
                    }
                    _ => {}
                }
            }

            // Upsert stations
            for s in data.stations() {
                if let Some(slot) = self.stations.iter_mut().find(|e| e.id() == s.id()) {
                    *slot = s.clone();
                } else {
                    self.stations.push(s.clone());
                }
            }

            // Replace volatile data atomically
            self.schedules = data.schedules().to_vec();
            self.trips = data.trip_legs().to_vec();
            self.schedules_by_route = data.schedules_by_route().clone();

            // Auto-create internal stations for unmapped source stations.
            let mut new_internal_stations = Vec::new();
            for s in data.stations() {
                let already_mapped = self
                    .station_mappings
                    .iter()
                    .any(|m| m.source == data.source() && &m.source_id == s.id());
                if !already_mapped {
                    let internal_id = InternalStationId::from(self.next_internal_id);
                    self.next_internal_id += 1;
                    self.internal_stations.push(InternalStation::new(
                        internal_id.clone(),
                        s.name().to_owned(),
                        s.lat(),
                        s.lon(),
                    ));
                    self.station_mappings.push(StationMapping {
                        source: data.source().to_owned(),
                        source_id: s.id().clone(),
                        internal_id: internal_id.clone(),
                    });
                    new_internal_stations.push(internal_id);
                }
            }

            TimetableImportResult {
                station_changes,
                new_internal_stations,
            }
        }

        fn all_stations(&self) -> Vec<ImportedStation> {
            self.stations.clone()
        }
        fn all_schedules(&self) -> Vec<ImportedSchedule> {
            self.schedules.clone()
        }
        fn all_trips(&self) -> Vec<ImportedTripLeg> {
            self.trips.clone()
        }
        fn schedules_by_route(&self) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
            self.schedules_by_route.clone()
        }
        fn internal_stations(&self) -> Vec<InternalStation> {
            self.internal_stations.clone()
        }
        fn station_mappings(&self) -> Vec<StationMapping> {
            self.station_mappings.clone()
        }
    }

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

    #[derive(Debug, Clone, Constructor)]
    struct TestImporter {
        stations: Vec<ImportedStation>,
        trips: Vec<ImportedTripLeg>,
        schedules: Vec<ImportedSchedule>,
        schedules_by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
        source: String,
    }

    impl ImportTrainData for TestImporter {
        fn stations(&self) -> &[ImportedStation] {
            &self.stations
        }

        fn trip_legs(&self) -> &[ImportedTripLeg] {
            &self.trips
        }

        fn schedules(&self) -> &[ImportedSchedule] {
            &self.schedules
        }

        fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
            &self.schedules_by_route
        }

        fn source(&self) -> &str {
            &self.source
        }
    }

    #[test]
    fn ingest_builds_graph() {
        let stations = vec![station("A"), station("B")];
        let schedules = vec![ImportedSchedule::new(
            ImportedScheduleId::from("schedule_1".to_string()),
            vec!["20260102".to_string()],
        )];
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("A"),
            sid("B"),
            100,
            200,
        )];
        let schedules_by_route = HashMap::new();
        let importer = TestImporter::new(
            stations,
            trips,
            schedules,
            schedules_by_route,
            "source".to_string(),
        );

        let mut service = ScheduleService::new(InMemoryTestRepository::empty());
        service.ingest(&importer);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 1);
    }

    #[test]
    fn empty_repository_produces_empty_graph() {
        let service = ScheduleService::new(InMemoryTestRepository::empty());
        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_origin_is_skipped() {
        let stations = vec![station("A"), station("B")];
        let schedules = vec![ImportedSchedule::new(
            ImportedScheduleId::from("schedule_1".to_string()),
            vec!["20260102".to_string()],
        )];
        // "X" is not in the station list
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("X"),
            sid("B"),
            100,
            200,
        )];
        let schedules_by_route = HashMap::new();
        let importer = TestImporter::new(
            stations,
            trips,
            schedules,
            schedules_by_route,
            "source".to_string(),
        );

        let mut service = ScheduleService::new(InMemoryTestRepository::empty());
        service.ingest(&importer);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_destination_is_skipped() {
        let stations = vec![station("A"), station("B")];
        let schedules = vec![ImportedSchedule::new(
            ImportedScheduleId::from("schedule_1".to_string()),
            vec!["20260102".to_string()],
        )];
        // "X" is not in the station list
        let trips = vec![ImportedTripLeg::new(
            route("R1"),
            sid("A"),
            sid("X"),
            100,
            200,
        )];
        let schedules_by_route = HashMap::new();
        let importer = TestImporter::new(
            stations,
            trips,
            schedules,
            schedules_by_route,
            "source".to_string(),
        );

        let mut service = ScheduleService::new(InMemoryTestRepository::empty());
        service.ingest(&importer);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn multiple_trips_from_same_origin_are_all_indexed() {
        let stations = vec![station("A"), station("B"), station("C")];
        let schedules = vec![ImportedSchedule::new(
            ImportedScheduleId::from("schedule_1".to_string()),
            vec!["20260102".to_string()],
        )];
        let trips = vec![
            ImportedTripLeg::new(route("R1"), sid("A"), sid("B"), 100, 200),
            ImportedTripLeg::new(route("R1"), sid("A"), sid("C"), 300, 400),
            ImportedTripLeg::new(route("R1"), sid("A"), sid("B"), 500, 600),
        ];
        let schedules_by_route = HashMap::new();
        let importer = TestImporter::new(
            stations,
            trips,
            schedules,
            schedules_by_route,
            "source".to_string(),
        );

        let mut service = ScheduleService::new(InMemoryTestRepository::empty());
        service.ingest(&importer);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 3);
    }
}
