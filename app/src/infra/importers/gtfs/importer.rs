use std::collections::HashMap;

use crate::app::schedule::{ImportedRouteId, ImportedStation, ImportedStationId, ImportedTrip};

use super::{
    GTFSExceptionType, GTFSLocationType, GTFSRawCalendarDate, GTFSRawStop, GTFSRawStopTime,
    GTFSRawTrip, GTFSRouteId, GTFSService, GTFSServiceId, GTFSStation, GTFSStationId, GTFSStopId,
    GTFSTripId, GTFSTripLeg, ParseGTFS,
};

pub struct GTFSImporter {
    stations: Vec<GTFSStation>,
    trips: Vec<GTFSTripLeg>,
    services: Vec<GTFSService>,
    services_by_route: HashMap<GTFSRouteId, Vec<GTFSServiceId>>,
}

impl GTFSImporter {
    pub fn from_parser(parser: &impl ParseGTFS) -> Self {
        let stations = build_stations(parser.stops());
        let trips = build_trip_legs(parser.stop_times(), parser.trips());
        let services = build_services(parser.calendar_dates());
        let services_by_route = build_services_by_route(parser.trips());

        Self {
            stations,
            trips,
            services,
            services_by_route,
        }
    }

    pub fn stations(&self) -> Vec<ImportedStation> {
        reconcile_stations(&self.stations)
    }

    pub fn trips(&self) -> Vec<ImportedTrip> {
        reconcile_trips(&self.stations, &self.trips)
    }

    pub fn schedules(&self) -> &[GTFSService] {
        &self.services
    }

    pub fn schedules_by_route(&self) -> &HashMap<GTFSRouteId, Vec<GTFSServiceId>> {
        &self.services_by_route
    }
}

/// Groups flat stop rows into `GTFSStation` values.
///
/// Rows where `location_type == Station` become the station entries (holding
/// name and coordinates). Rows where `location_type == Stop` are attached to
/// their `parent_station`.
fn build_stations(stops: &[GTFSRawStop]) -> Vec<GTFSStation> {
    // Map: parent station raw id → child stop ids.
    let mut children: HashMap<&str, Vec<GTFSStopId>> = HashMap::new();
    for stop in stops
        .iter()
        .filter(|s| s.location_type() == GTFSLocationType::Stop)
    {
        if let Some(parent) = stop.parent_station() {
            children
                .entry(parent.as_str())
                .or_default()
                .push(stop.id().clone());
        }
    }

    stops
        .iter()
        .filter(|s| s.location_type() == GTFSLocationType::Station)
        .map(|s| {
            let platform_stops = children.remove(s.id().as_str()).unwrap_or_default();
            GTFSStation::new(
                GTFSStationId::from(s.id().as_str().to_owned()),
                s.name().to_owned(),
                s.lat(),
                s.lon(),
                platform_stops,
            )
        })
        .collect()
}

/// Expands flat stop-time rows into all (origin → destination) `GTFSTripLeg` pairs.
///
/// For each GTFS trip the stop times are sorted by `stop_sequence` and then
/// every ordered pair `(i, j)` with `i < j` is emitted as a leg — so a
/// three-stop trip A → B → C produces legs A→B, A→C, and B→C.
fn build_trip_legs(stop_times: &[GTFSRawStopTime], trips_raw: &[GTFSRawTrip]) -> Vec<GTFSTripLeg> {
    // trip_id → route_id (borrows from trips_raw)
    let route_by_trip: HashMap<&GTFSTripId, &GTFSRouteId> = trips_raw
        .iter()
        .map(|t| (t.trip_id(), t.route_id()))
        .collect();

    // Group stop times by trip, then sort each group by sequence.
    let mut by_trip: HashMap<&GTFSTripId, Vec<&GTFSRawStopTime>> = HashMap::new();
    for st in stop_times {
        by_trip.entry(st.trip_id()).or_default().push(st);
    }
    for stops in by_trip.values_mut() {
        stops.sort_by_key(|st| st.stop_sequence());
    }

    let mut legs = vec![];
    for (trip_id, stops) in &by_trip {
        let Some(route) = route_by_trip.get(trip_id) else {
            continue;
        };
        for (i, origin) in stops.iter().enumerate() {
            for dest in &stops[i + 1..] {
                legs.push(GTFSTripLeg::new(
                    (*route).clone(),
                    origin.stop_id().clone(),
                    dest.stop_id().clone(),
                    origin.departure(),
                    dest.arrival(),
                ));
            }
        }
    }
    legs
}

/// Builds `GTFSService` schedules from raw calendar-date rows.
///
/// Only `ServiceAdded` rows contribute dates; `ServiceRemoved` rows are
/// intentionally ignored here — they express exceptions to a base calendar
/// that this feed does not include.
fn build_services(calendar_dates: &[GTFSRawCalendarDate]) -> Vec<GTFSService> {
    let mut by_service: HashMap<GTFSServiceId, Vec<String>> = HashMap::new();
    for date in calendar_dates
        .iter()
        .filter(|d| d.exception_type() == GTFSExceptionType::ServiceAdded)
    {
        by_service
            .entry(date.service_id().clone())
            .or_default()
            .push(date.date().to_owned());
    }
    by_service
        .into_iter()
        .map(|(id, dates)| GTFSService::new(id, dates))
        .collect()
}

/// Builds the route → services index from raw trip rows.
fn build_services_by_route(trips_raw: &[GTFSRawTrip]) -> HashMap<GTFSRouteId, Vec<GTFSServiceId>> {
    let mut by_route: HashMap<GTFSRouteId, Vec<GTFSServiceId>> = HashMap::new();
    for trip in trips_raw {
        by_route
            .entry(trip.route_id().clone())
            .or_default()
            .push(trip.service_id().clone());
    }
    by_route
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
    // Build a reverse map: GTFSStopId → GTFSStationId, because GTFSTripLeg
    // references platform stops while ImportedTrip references stations.
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
    use super::*;

    // ── test helpers ────────────────────────────────────────────────────────

    fn sid(id: &str) -> GTFSStopId {
        GTFSStopId::from(id.to_owned())
    }

    /// A station row (location_type = 1, no parent).
    fn raw_station(id: &str, name: &str) -> GTFSRawStop {
        GTFSRawStop::new(
            sid(id),
            name.to_owned(),
            0.0,
            0.0,
            GTFSLocationType::Station,
            None,
        )
    }

    /// A platform/stop row (location_type = 0, with parent).
    fn raw_stop(id: &str, parent: &str) -> GTFSRawStop {
        GTFSRawStop::new(
            sid(id),
            id.to_owned(),
            0.0,
            0.0,
            GTFSLocationType::Stop,
            Some(sid(parent)),
        )
    }

    fn raw_trip(trip_id: &str, route_id: &str, service_id: &str) -> GTFSRawTrip {
        GTFSRawTrip::new(
            GTFSTripId::from(trip_id.to_owned()),
            GTFSRouteId::from(route_id.to_owned()),
            GTFSServiceId::from(service_id.to_owned()),
        )
    }

    fn raw_stop_time(
        trip_id: &str,
        stop_id: &str,
        arrival: usize,
        departure: usize,
        seq: usize,
    ) -> GTFSRawStopTime {
        GTFSRawStopTime::new(
            GTFSTripId::from(trip_id.to_owned()),
            arrival,
            departure,
            sid(stop_id),
            seq,
        )
    }

    struct StubParser {
        stops: Vec<GTFSRawStop>,
        stop_times: Vec<GTFSRawStopTime>,
        trips: Vec<GTFSRawTrip>,
        calendar_dates: Vec<GTFSRawCalendarDate>,
    }

    impl ParseGTFS for StubParser {
        fn stops(&self) -> &[GTFSRawStop] {
            &self.stops
        }
        fn stop_times(&self) -> &[GTFSRawStopTime] {
            &self.stop_times
        }
        fn trips(&self) -> &[GTFSRawTrip] {
            &self.trips
        }
        fn calendar_dates(&self) -> &[GTFSRawCalendarDate] {
            &self.calendar_dates
        }
    }

    // ── stations ────────────────────────────────────────────────────────────

    #[test]
    fn stations_are_converted_from_gtfs() {
        let parser = StubParser {
            stops: vec![
                raw_station("S1", "Paris Nord"),
                raw_stop("S1-A", "S1"),
                raw_stop("S1-B", "S1"),
                raw_station("S2", "Lyon Perrache"),
                raw_stop("S2-A", "S2"),
            ],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![],
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
    fn stop_rows_without_parent_are_not_exposed_as_stations() {
        // A stop row with no parent_station should not appear as a station.
        let parser = StubParser {
            stops: vec![
                raw_station("S1", "Paris Nord"),
                GTFSRawStop::new(
                    sid("orphan"),
                    "Orphan".to_owned(),
                    0.0,
                    0.0,
                    GTFSLocationType::Stop,
                    None,
                ),
            ],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![],
        };
        let result = GTFSImporter::from_parser(&parser).stations();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), &ImportedStationId::from("S1".to_owned()));
    }

    // ── trips ────────────────────────────────────────────────────────────────

    #[test]
    fn trips_resolve_stops_to_their_parent_station() {
        let parser = StubParser {
            stops: vec![
                raw_station("S1", "Paris Nord"),
                raw_stop("S1-A", "S1"),
                raw_stop("S1-B", "S1"),
                raw_station("S2", "Lyon Perrache"),
                raw_stop("S2-A", "S2"),
            ],
            // Trip uses platform S1-B (child of S1) → S2-A (child of S2)
            stop_times: vec![
                raw_stop_time("T1", "S1-B", 0, 800, 0),
                raw_stop_time("T1", "S2-A", 1200, 0, 1),
            ],
            trips: vec![raw_trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
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
            stops: vec![raw_station("S1", "Paris Nord"), raw_stop("S1-A", "S1")],
            // S2-X belongs to no station → leg must be dropped
            stop_times: vec![
                raw_stop_time("T1", "S1-A", 0, 800, 0),
                raw_stop_time("T1", "S2-X", 1200, 0, 1),
            ],
            trips: vec![raw_trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let result = GTFSImporter::from_parser(&parser).trips();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn multiple_stops_from_same_station_map_to_same_station_id() {
        let parser = StubParser {
            stops: vec![
                raw_station("S1", "Paris Nord"),
                raw_stop("S1-A", "S1"),
                raw_stop("S1-B", "S1"),
                raw_station("S2", "Lyon Perrache"),
                raw_stop("S2-A", "S2"),
                raw_stop("S2-B", "S2"),
            ],
            stop_times: vec![
                raw_stop_time("T1", "S1-A", 0, 800, 0),
                raw_stop_time("T1", "S2-A", 1200, 0, 1),
                raw_stop_time("T2", "S1-B", 0, 900, 0),
                raw_stop_time("T2", "S2-B", 1300, 0, 1),
            ],
            trips: vec![raw_trip("T1", "R1", "SVC1"), raw_trip("T2", "R1", "SVC2")],
            calendar_dates: vec![],
        };
        let mut result = GTFSImporter::from_parser(&parser).trips();
        result.sort();

        let expected_origin = ImportedStationId::from("S1".to_owned());
        let expected_dest = ImportedStationId::from("S2".to_owned());
        for t in &result {
            assert_eq!(t.origin(), &expected_origin);
            assert_eq!(t.destination(), &expected_dest);
        }
    }

    #[test]
    fn three_stop_trip_expands_into_all_pairs() {
        // A → B → C should yield legs A→B, A→C, B→C
        let parser = StubParser {
            stops: vec![
                raw_station("SA", "Station A"),
                raw_stop("A", "SA"),
                raw_station("SB", "Station B"),
                raw_stop("B", "SB"),
                raw_station("SC", "Station C"),
                raw_stop("C", "SC"),
            ],
            stop_times: vec![
                raw_stop_time("T1", "A", 0, 600, 0),
                raw_stop_time("T1", "B", 1200, 1200, 1),
                raw_stop_time("T1", "C", 1800, 0, 2),
            ],
            trips: vec![raw_trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let mut result = GTFSImporter::from_parser(&parser).trips();
        result.sort();

        assert_eq!(result.len(), 3);
        let origins: Vec<_> = result.iter().map(|t| t.origin().clone()).collect();
        let dests: Vec<_> = result.iter().map(|t| t.destination().clone()).collect();
        assert!(origins.contains(&ImportedStationId::from("SA".to_owned())));
        assert!(dests.contains(&ImportedStationId::from("SC".to_owned())));
    }

    // ── services ─────────────────────────────────────────────────────────────

    #[test]
    fn service_added_dates_are_collected() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![
                GTFSRawCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260501".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSRawCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260508".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSRawCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260515".to_owned(),
                    GTFSExceptionType::ServiceRemoved,
                ),
            ],
        };
        let importer = GTFSImporter::from_parser(&parser);
        let services = importer.schedules();

        assert_eq!(services.len(), 1);
        assert_eq!(services[0].id(), &GTFSServiceId::from("SVC1".to_owned()));
        let mut dates = services[0].dates().to_vec();
        dates.sort();
        assert_eq!(dates, vec!["20260501", "20260508"]);
    }

    #[test]
    fn services_by_route_groups_service_ids_per_route() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![
                raw_trip("T1", "R1", "SVC1"),
                raw_trip("T2", "R1", "SVC2"),
                raw_trip("T3", "R2", "SVC3"),
            ],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser);
        let by_route = importer.schedules_by_route();

        let r1 = GTFSRouteId::from("R1".to_owned());
        let r2 = GTFSRouteId::from("R2".to_owned());
        let mut svc_r1 = by_route[&r1].clone();
        svc_r1.sort();
        assert_eq!(
            svc_r1,
            vec![
                GTFSServiceId::from("SVC1".to_owned()),
                GTFSServiceId::from("SVC2".to_owned()),
            ]
        );
        assert_eq!(by_route[&r2], vec![GTFSServiceId::from("SVC3".to_owned())]);
    }
}
