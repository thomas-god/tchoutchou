use std::collections::HashMap;

use crate::app::schedule::{ImportedRouteId, ImportedStation, ImportedStationId, ImportedTrip};

use super::{GTFSStation, GTFSStationId, GTFSStopId, GTFSTripLeg, ParseGTFS};

pub struct GTFSImporter {
    stations: Vec<GTFSStation>,
    trips: Vec<GTFSTripLeg>,
}

impl GTFSImporter {
    pub fn from_parser(parser: &impl ParseGTFS) -> Self {
        Self {
            stations: parser.stations().to_vec(),
            trips: parser.trips().to_vec(),
        }
    }

    pub fn stations(&self) -> Vec<ImportedStation> {
        reconcile_stations(&self.stations)
    }

    pub fn trips(&self) -> Vec<ImportedTrip> {
        reconcile_trips(&self.stations, &self.trips)
    }
}

fn reconcile_stations(stations: &[GTFSStation]) -> Vec<ImportedStation> {
    stations
        .iter()
        .map(|s| {
            ImportedStation::new(
                ImportedStationId::from(s.id().as_str().to_owned()),
                s.name().to_owned(),
                s.lat(),
                s.lon(),
            )
        })
        .collect()
}

fn reconcile_trips(stations: &[GTFSStation], trips: &[GTFSTripLeg]) -> Vec<ImportedTrip> {
    // Build a reverse map: GTFSStopId → GTFSStationId, because GTFSTrip references
    // stops while ImportedTrip references stations.
    let stop_to_station: HashMap<&GTFSStopId, &GTFSStationId> = stations
        .iter()
        .flat_map(|s| s.stops().iter().map(move |stop| (stop, s.id())))
        .collect();

    trips
        .iter()
        .filter_map(|trip| {
            let origin_station = stop_to_station.get(trip.origin())?;
            let destination_station = stop_to_station.get(trip.destination())?;
            Some(ImportedTrip::new(
                ImportedRouteId::from(trip.route().as_str().to_owned()),
                ImportedStationId::from(origin_station.as_str().to_owned()),
                ImportedStationId::from(destination_station.as_str().to_owned()),
                trip.departure(),
                trip.arrival(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::infra::importers::gtfs::{GTFSRouteId, GTFSService, GTFSServiceId};

    use super::*;

    fn stop(id: &str) -> GTFSStopId {
        GTFSStopId::from(id.to_owned())
    }

    fn route(id: &str) -> GTFSRouteId {
        GTFSRouteId::from(id.to_owned())
    }

    fn station_id(id: &str) -> GTFSStationId {
        GTFSStationId::from(id.to_owned())
    }

    fn station(id: &str, name: &str, stops: Vec<GTFSStopId>) -> GTFSStation {
        GTFSStation::new(station_id(id), name.to_owned(), 0.0, 0.0, stops)
    }

    fn trip(
        route: GTFSRouteId,
        origin: GTFSStopId,
        destination: GTFSStopId,
        dep: usize,
        arr: usize,
    ) -> GTFSTripLeg {
        GTFSTripLeg::new(route, origin, destination, dep, arr)
    }

    struct StubParser {
        stations: Vec<GTFSStation>,
        trips: Vec<GTFSTripLeg>,
        services: Vec<GTFSService>,
        services_by_route: HashMap<GTFSRouteId, Vec<GTFSServiceId>>,
    }

    impl ParseGTFS for StubParser {
        fn stations(&self) -> &[GTFSStation] {
            &self.stations
        }
        fn trips(&self) -> &[GTFSTripLeg] {
            &self.trips
        }
        fn schedules(&self) -> &[GTFSService] {
            &self.services
        }
        fn schedules_by_route(&self) -> &HashMap<GTFSRouteId, Vec<GTFSServiceId>> {
            &self.services_by_route
        }
    }

    #[test]
    fn stations_are_converted_from_gtfs() {
        let parser = StubParser {
            stations: vec![
                station("S1", "Paris Nord", vec![stop("S1-A"), stop("S1-B")]),
                station("S2", "Lyon Perrache", vec![stop("S2-A")]),
            ],
            trips: vec![],
            services: vec![],
            services_by_route: HashMap::new(),
        };
        let mut result = GTFSImporter::from_parser(&parser).stations();
        result.sort_by_key(|s| s.id().clone());

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ImportedStation::new(
                ImportedStationId::from("S1".to_owned()),
                "Paris Nord".to_owned(),
                0.0,
                0.0
            )
        );
        assert_eq!(
            result[1],
            ImportedStation::new(
                ImportedStationId::from("S2".to_owned()),
                "Lyon Perrache".to_owned(),
                0.0,
                0.0
            )
        );
    }

    #[test]
    fn trips_resolve_stops_to_their_parent_station() {
        let parser = StubParser {
            stations: vec![
                station("S1", "Paris Nord", vec![stop("S1-A"), stop("S1-B")]),
                station("S2", "Lyon Perrache", vec![stop("S2-A")]),
            ],
            // Trip uses stop S1-B (child of S1) → stop S2-A (child of S2)
            trips: vec![trip(route("R1"), stop("S1-B"), stop("S2-A"), 800, 1200)],
            services: vec![],
            services_by_route: HashMap::new(),
        };
        let result = GTFSImporter::from_parser(&parser).trips();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            ImportedTrip::new(
                ImportedRouteId::from("R1".to_owned()),
                ImportedStationId::from("S1".to_owned()),
                ImportedStationId::from("S2".to_owned()),
                800,
                1200,
            )
        );
    }

    #[test]
    fn trips_with_unknown_stops_are_dropped() {
        let parser = StubParser {
            stations: vec![station("S1", "Paris Nord", vec![stop("S1-A")])],
            // S2-X belongs to no station
            trips: vec![trip(route("R1"), stop("S1-A"), stop("S2-X"), 800, 1200)],
            services: vec![],
            services_by_route: HashMap::new(),
        };
        let result = GTFSImporter::from_parser(&parser).trips();

        assert_eq!(result, vec![]);
    }

    #[test]
    fn multiple_stops_from_same_station_map_to_same_station_id() {
        let parser = StubParser {
            stations: vec![
                station("S1", "Paris Nord", vec![stop("S1-A"), stop("S1-B")]),
                station("S2", "Lyon Perrache", vec![stop("S2-A"), stop("S2-B")]),
            ],
            trips: vec![
                trip(route("R1"), stop("S1-A"), stop("S2-A"), 800, 1200),
                trip(route("R1"), stop("S1-B"), stop("S2-B"), 900, 1300),
            ],
            services: vec![],
            services_by_route: HashMap::new(),
        };
        let mut result = GTFSImporter::from_parser(&parser).trips();
        result.sort();

        let expected_station_1 = ImportedStationId::from("S1".to_owned());
        let expected_station_2 = ImportedStationId::from("S2".to_owned());
        for t in &result {
            assert_eq!(t.origin(), &expected_station_1);
            assert_eq!(t.destination(), &expected_station_2);
        }
    }
}
