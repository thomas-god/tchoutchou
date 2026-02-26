use std::collections::HashMap;

use derive_more::{Constructor, From};

use crate::domain::optim::{Graph, StationId, Trip};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

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
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedScheduleId(String);

#[derive(Debug, Clone, Constructor, PartialEq)]
pub struct ImportedSchedule {
    id: ImportedScheduleId,
    dates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedRouteId(String);

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTrip {
    route: ImportedRouteId,
    origin: ImportedStationId,
    destination: ImportedStationId,
    departure: usize,
    arrival: usize,
}

impl ImportedTrip {
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
    fn trip_legs(&self) -> &[ImportedTrip];
    fn schedules(&self) -> &[ImportedSchedule];
    fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>>;
}

/// Persistence contract for stations and trips. Infrastructure crates provide
/// concrete implementations (in-memory, database, …).
pub trait StationAndTripRepository {
    fn save_stations(&mut self, stations: &[ImportedStation]);
    fn save_schedules(&mut self, schedules: &[ImportedSchedule]);
    fn save_trips(&mut self, trips: &[ImportedTrip]);
    fn all_stations(&self) -> Vec<ImportedStation>;
    fn all_schedules(&self) -> Vec<ImportedSchedule>;
    fn all_trips(&self) -> Vec<ImportedTrip>;
}

/// Application service that aggregates data from various importers, persists it
/// through a [`StationAndTripRepository`], and exposes a [`Graph`] ready for the
/// optimisation algorithms in [`crate::domain::optim`].
pub struct ScheduleService<R: StationAndTripRepository> {
    repository: R,
}

impl<R: StationAndTripRepository> ScheduleService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    /// Feed stations, schedules and trips from any importer into the repository.
    pub fn ingest(
        &mut self,
        stations: &[ImportedStation],
        schedules: &[ImportedSchedule],
        trips: &[ImportedTrip],
    ) {
        self.repository.save_schedules(schedules);
        self.repository.save_stations(stations);
        self.repository.save_trips(trips);
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
fn build_graph(stations: &[ImportedStation], trips: &[ImportedTrip]) -> Graph {
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

    struct InMemoryRepository {
        stations: Vec<ImportedStation>,
        schedules: Vec<ImportedSchedule>,
        trips: Vec<ImportedTrip>,
    }

    impl InMemoryRepository {
        fn empty() -> Self {
            Self {
                stations: vec![],
                schedules: vec![],
                trips: vec![],
            }
        }
    }

    impl StationAndTripRepository for InMemoryRepository {
        fn save_stations(&mut self, stations: &[ImportedStation]) {
            self.stations.extend_from_slice(stations);
        }
        fn save_schedules(&mut self, schedules: &[ImportedSchedule]) {
            self.schedules.extend_from_slice(schedules);
        }
        fn save_trips(&mut self, trips: &[ImportedTrip]) {
            self.trips.extend_from_slice(trips);
        }
        fn all_stations(&self) -> Vec<ImportedStation> {
            self.stations.clone()
        }
        fn all_schedules(&self) -> Vec<ImportedSchedule> {
            self.schedules.clone()
        }
        fn all_trips(&self) -> Vec<ImportedTrip> {
            self.trips.clone()
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

    #[test]
    fn ingest_builds_graph() {
        let stations = vec![station("A"), station("B")];
        let schedules = vec![ImportedSchedule::new(
            ImportedScheduleId::from("schedule_1".to_string()),
            vec!["20260102".to_string()],
        )];
        let trips = vec![ImportedTrip::new(route("R1"), sid("A"), sid("B"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &schedules, &trips);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 1);
    }

    #[test]
    fn empty_repository_produces_empty_graph() {
        let service = ScheduleService::new(InMemoryRepository::empty());
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
        let trips = vec![ImportedTrip::new(route("R1"), sid("X"), sid("B"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &schedules, &trips);

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
        let trips = vec![ImportedTrip::new(route("R1"), sid("A"), sid("X"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &schedules, &trips);

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
            ImportedTrip::new(route("R1"), sid("A"), sid("B"), 100, 200),
            ImportedTrip::new(route("R1"), sid("A"), sid("C"), 300, 400),
            ImportedTrip::new(route("R1"), sid("A"), sid("B"), 500, 600),
        ];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &schedules, &trips);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 3);
    }
}
