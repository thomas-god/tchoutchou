use std::collections::HashMap;

use crate::domain::optim::{Graph, StationId, Trip};
use crate::infra::importers::{ImportedStation, ImportedStationId, ImportedTrip};

/// Persistence contract for stations and trips. Infrastructure crates provide
/// concrete implementations (in-memory, database, …).
pub trait StationAndTripRepository {
    fn save_stations(&mut self, stations: &[ImportedStation]);
    fn save_trips(&mut self, trips: &[ImportedTrip]);
    fn all_stations(&self) -> Vec<ImportedStation>;
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

    /// Feed stations and trips from any importer into the repository.
    pub fn ingest(&mut self, stations: &[ImportedStation], trips: &[ImportedTrip]) {
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
        trips: Vec<ImportedTrip>,
    }

    impl InMemoryRepository {
        fn empty() -> Self {
            Self {
                stations: vec![],
                trips: vec![],
            }
        }
    }

    impl StationAndTripRepository for InMemoryRepository {
        fn save_stations(&mut self, stations: &[ImportedStation]) {
            self.stations.extend_from_slice(stations);
        }
        fn save_trips(&mut self, trips: &[ImportedTrip]) {
            self.trips.extend_from_slice(trips);
        }
        fn all_stations(&self) -> Vec<ImportedStation> {
            self.stations.clone()
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

    fn sid(id: &str) -> ImportedStationId {
        ImportedStationId::from(id.to_owned())
    }

    #[test]
    fn ingest_builds_graph() {
        let stations = vec![station("A"), station("B")];
        let trips = vec![ImportedTrip::new(sid("A"), sid("B"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &trips);

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
        // "X" is not in the station list
        let trips = vec![ImportedTrip::new(sid("X"), sid("B"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &trips);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
        assert_eq!(graph.trips_from(StationId::from(1)).len(), 0);
    }

    #[test]
    fn trip_with_unknown_destination_is_skipped() {
        let stations = vec![station("A"), station("B")];
        // "X" is not in the station list
        let trips = vec![ImportedTrip::new(sid("A"), sid("X"), 100, 200)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &trips);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 0);
    }

    #[test]
    fn multiple_trips_from_same_origin_are_all_indexed() {
        let stations = vec![station("A"), station("B"), station("C")];
        let trips = vec![
            ImportedTrip::new(sid("A"), sid("B"), 100, 200),
            ImportedTrip::new(sid("A"), sid("C"), 300, 400),
            ImportedTrip::new(sid("A"), sid("B"), 500, 600),
        ];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &trips);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 3);
    }

    #[test]
    fn multiple_ingestions_accumulate() {
        let stations = vec![station("A"), station("B")];
        let first_batch = vec![ImportedTrip::new(sid("A"), sid("B"), 100, 200)];
        let second_batch = vec![ImportedTrip::new(sid("A"), sid("B"), 300, 400)];

        let mut service = ScheduleService::new(InMemoryRepository::empty());
        service.ingest(&stations, &[]);
        service.ingest(&[], &first_batch);
        service.ingest(&[], &second_batch);

        let graph = service.graph();
        assert_eq!(graph.trips_from(StationId::from(0)).len(), 2);
    }
}
