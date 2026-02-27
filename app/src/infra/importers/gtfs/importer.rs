use std::collections::HashMap;

use derive_more::{Constructor, From};

use crate::app::schedule::{
    ImportTrainData, ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation,
    ImportedStationId, ImportedTripLeg,
};

use super::{
    GTFSCalendarDate, GTFSExceptionType, GTFSLocationType, GTFSRouteId, GTFSServiceId, GTFSStop,
    GTFSStopId, GTFSStopTime, GTFSTrip, GTFSTripId, ParseGTFS,
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
struct GTFSStationId(String);

impl GTFSStationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A station can contain several, possibly abstract, stops. For example `GTFSStationId` "Paris Gare
/// de Lyon" can contain `GTFSStopId`s "Paris Gare de Lyon - TGV" and "Paris Gare de Lyon - OUIGO"
/// amongst others.
#[derive(Debug, Clone, PartialEq, Constructor)]
struct GTFSStation {
    id: GTFSStationId,
    name: String,
    lat: f64,
    lon: f64,
    stops: Vec<GTFSStopId>,
}

impl GTFSStation {
    fn id(&self) -> &GTFSStationId {
        &self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn lat(&self) -> f64 {
        self.lat
    }
    fn lon(&self) -> f64 {
        self.lon
    }
    fn stops(&self) -> &[GTFSStopId] {
        &self.stops
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
struct GTFSTripLeg {
    route: GTFSRouteId,
    origin: GTFSStopId,
    destination: GTFSStopId,
    departure: usize,
    arrival: usize,
}

impl GTFSTripLeg {
    fn route(&self) -> &GTFSRouteId {
        &self.route
    }
    fn origin(&self) -> &GTFSStopId {
        &self.origin
    }
    fn destination(&self) -> &GTFSStopId {
        &self.destination
    }
    fn departure(&self) -> usize {
        self.departure
    }
    fn arrival(&self) -> usize {
        self.arrival
    }
}

pub struct GTFSImporter {
    stations: Vec<ImportedStation>,
    trips: Vec<ImportedTripLeg>,
    schedules: Vec<ImportedSchedule>,
    schedules_by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
    source: String,
}

impl GTFSImporter {
    pub fn from_parser(parser: &impl ParseGTFS, source: &str) -> Self {
        let gtfs_stations = build_stations(parser.stops());
        let gtfs_trips = build_trip_legs(parser.stop_times(), parser.trips());

        let stations = reconcile_stations(&gtfs_stations);
        let trips = reconcile_trips(&gtfs_stations, &gtfs_trips);
        let schedules = build_imported_schedules(parser.calendar_dates());
        let schedules_by_route = build_imported_schedules_by_route(parser.trips());

        Self {
            stations,
            trips,
            schedules,
            schedules_by_route,
            source: source.to_owned(),
        }
    }
}

impl ImportTrainData for GTFSImporter {
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

/// Groups flat stop rows into `GTFSStation` values.
///
/// Rows where `location_type == Station` become the station entries (holding
/// name and coordinates). Rows where `location_type == Stop` are attached to
/// their `parent_station`.
fn build_stations(stops: &[GTFSStop]) -> Vec<GTFSStation> {
    // Map: parent station id → child stop ids.
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
fn build_trip_legs(stop_times: &[GTFSStopTime], trips: &[GTFSTrip]) -> Vec<GTFSTripLeg> {
    // trip_id → route_id (borrows from trips)
    let route_by_trip: HashMap<&GTFSTripId, &GTFSRouteId> =
        trips.iter().map(|t| (t.trip_id(), t.route_id())).collect();

    // Group stop times by trip, then sort each group by sequence.
    let mut by_trip: HashMap<&GTFSTripId, Vec<&GTFSStopTime>> = HashMap::new();
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

/// Builds `ImportedSchedule` values from calendar-date rows.
///
/// Only `ServiceAdded` rows contribute dates; `ServiceRemoved` rows are
/// intentionally ignored — they express exceptions to a base calendar that
/// this feed does not include.
fn build_imported_schedules(calendar_dates: &[GTFSCalendarDate]) -> Vec<ImportedSchedule> {
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
        .map(|(id, dates)| {
            ImportedSchedule::new(ImportedScheduleId::from(id.as_str().to_owned()), dates)
        })
        .collect()
}

/// Builds the route → schedule-ids index from trip rows.
///
/// Duplicate `service_id` entries for the same route are deduplicated — a
/// service appearing on a route is a binary fact; listing it twice carries no
/// additional meaning.
fn build_imported_schedules_by_route(
    trips: &[GTFSTrip],
) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
    let mut by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>> = HashMap::new();
    for trip in trips {
        by_route
            .entry(ImportedRouteId::from(trip.route_id().as_str().to_owned()))
            .or_default()
            .push(ImportedScheduleId::from(
                trip.service_id().as_str().to_owned(),
            ));
    }
    for services in by_route.values_mut() {
        services.sort();
        services.dedup();
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

fn reconcile_trips(stations: &[GTFSStation], trips: &[GTFSTripLeg]) -> Vec<ImportedTripLeg> {
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
            if origin_station == destination_station {
                return None;
            }
            Some(ImportedTripLeg::new(
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
    use crate::app::schedule::ImportTrainData;

    use super::*;

    // ── test helpers ────────────────────────────────────────────────────────

    fn sid(id: &str) -> GTFSStopId {
        GTFSStopId::from(id.to_owned())
    }

    /// A station row (location_type = 1, no parent).
    fn station(id: &str, name: &str) -> GTFSStop {
        GTFSStop::new(
            sid(id),
            name.to_owned(),
            0.0,
            0.0,
            GTFSLocationType::Station,
            None,
        )
    }

    /// A platform/stop row (location_type = 0, with parent).
    fn stop(id: &str, parent: &str) -> GTFSStop {
        GTFSStop::new(
            sid(id),
            id.to_owned(),
            0.0,
            0.0,
            GTFSLocationType::Stop,
            Some(sid(parent)),
        )
    }

    fn trip(trip_id: &str, route_id: &str, service_id: &str) -> GTFSTrip {
        GTFSTrip::new(
            GTFSTripId::from(trip_id.to_owned()),
            GTFSRouteId::from(route_id.to_owned()),
            GTFSServiceId::from(service_id.to_owned()),
        )
    }

    fn stop_time(
        trip_id: &str,
        stop_id: &str,
        arrival: usize,
        departure: usize,
        seq: usize,
    ) -> GTFSStopTime {
        GTFSStopTime::new(
            GTFSTripId::from(trip_id.to_owned()),
            arrival,
            departure,
            sid(stop_id),
            seq,
        )
    }

    struct StubParser {
        stops: Vec<GTFSStop>,
        stop_times: Vec<GTFSStopTime>,
        trips: Vec<GTFSTrip>,
        calendar_dates: Vec<GTFSCalendarDate>,
    }

    impl ParseGTFS for StubParser {
        fn stops(&self) -> &[GTFSStop] {
            &self.stops
        }
        fn stop_times(&self) -> &[GTFSStopTime] {
            &self.stop_times
        }
        fn trips(&self) -> &[GTFSTrip] {
            &self.trips
        }
        fn calendar_dates(&self) -> &[GTFSCalendarDate] {
            &self.calendar_dates
        }
    }

    // ── stations ────────────────────────────────────────────────────────────

    #[test]
    fn stations_are_converted_from_gtfs() {
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                stop("S1-B", "S1"),
                station("S2", "Lyon Perrache"),
                stop("S2-A", "S2"),
            ],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let mut result = importer.stations().to_vec();
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
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                GTFSStop::new(
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
        let importer = GTFSImporter::from_parser(&parser, "source");
        let result = importer.stations();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), &ImportedStationId::from("S1".to_owned()));
    }

    // ── trips ────────────────────────────────────────────────────────────────

    #[test]
    fn trips_resolve_stops_to_their_parent_station() {
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                stop("S1-B", "S1"),
                station("S2", "Lyon Perrache"),
                stop("S2-A", "S2"),
            ],
            stop_times: vec![
                stop_time("T1", "S1-B", 0, 800, 0),
                stop_time("T1", "S2-A", 1200, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let result = importer.trip_legs();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            ImportedTripLeg::new(
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
            stops: vec![station("S1", "Paris Nord"), stop("S1-A", "S1")],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "S2-X", 1200, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        assert!(importer.trip_legs().is_empty());
    }

    #[test]
    fn multiple_stops_from_same_station_map_to_same_station_id() {
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                stop("S1-B", "S1"),
                station("S2", "Lyon Perrache"),
                stop("S2-A", "S2"),
                stop("S2-B", "S2"),
            ],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "S2-A", 1200, 0, 1),
                stop_time("T2", "S1-B", 0, 900, 0),
                stop_time("T2", "S2-B", 1300, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1"), trip("T2", "R1", "SVC2")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let mut result = importer.trip_legs().to_vec();
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
        let parser = StubParser {
            stops: vec![
                station("SA", "Station A"),
                stop("A", "SA"),
                station("SB", "Station B"),
                stop("B", "SB"),
                station("SC", "Station C"),
                stop("C", "SC"),
            ],
            stop_times: vec![
                stop_time("T1", "A", 0, 600, 0),
                stop_time("T1", "B", 1200, 1200, 1),
                stop_time("T1", "C", 1800, 0, 2),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let mut result = importer.trip_legs().to_vec();
        result.sort();

        assert_eq!(result.len(), 3);
        let origins: Vec<_> = result.iter().map(|t| t.origin().clone()).collect();
        let dests: Vec<_> = result.iter().map(|t| t.destination().clone()).collect();
        assert!(origins.contains(&ImportedStationId::from("SA".to_owned())));
        assert!(dests.contains(&ImportedStationId::from("SC".to_owned())));
    }

    // ── schedules ────────────────────────────────────────────────────────────

    #[test]
    fn service_added_dates_are_collected() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260501".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260508".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260515".to_owned(),
                    GTFSExceptionType::ServiceRemoved,
                ),
            ],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let schedules = importer.schedules();

        assert_eq!(schedules.len(), 1);
        assert_eq!(
            schedules[0].id(),
            &ImportedScheduleId::from("SVC1".to_owned())
        );
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(dates, vec!["20260501", "20260508"]);
    }

    #[test]
    fn services_by_route_groups_service_ids_per_route() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![
                trip("T1", "R1", "SVC1"),
                trip("T2", "R1", "SVC2"),
                trip("T3", "R2", "SVC3"),
            ],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let by_route = importer.schedules_by_route();

        let r1 = ImportedRouteId::from("R1".to_owned());
        let r2 = ImportedRouteId::from("R2".to_owned());
        let mut svc_r1 = by_route[&r1].clone();
        svc_r1.sort();
        assert_eq!(
            svc_r1,
            vec![
                ImportedScheduleId::from("SVC1".to_owned()),
                ImportedScheduleId::from("SVC2".to_owned()),
            ]
        );
        assert_eq!(
            by_route[&r2],
            vec![ImportedScheduleId::from("SVC3".to_owned())]
        );
    }

    #[test]
    fn duplicate_service_id_for_same_route_is_deduplicated() {
        // Two trips on R1 share the same service_id; only one entry should appear.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![trip("T1", "R1", "SVC1"), trip("T2", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let by_route = importer.schedules_by_route();

        let r1 = ImportedRouteId::from("R1".to_owned());
        assert_eq!(
            by_route[&r1],
            vec![ImportedScheduleId::from("SVC1".to_owned())]
        );
    }

    #[test]
    fn all_service_removed_dates_produce_empty_schedules() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260501".to_owned(),
                    GTFSExceptionType::ServiceRemoved,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260508".to_owned(),
                    GTFSExceptionType::ServiceRemoved,
                ),
            ],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        assert!(importer.schedules().is_empty());
    }

    #[test]
    fn multiple_distinct_services_produce_multiple_schedules() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260501".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC2".to_owned()),
                    "20260502".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC2".to_owned()),
                    "20260509".to_owned(),
                    GTFSExceptionType::ServiceAdded,
                ),
            ],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let mut schedules = importer.schedules().to_vec();
        schedules.sort_by_key(|s| s.id().clone());

        assert_eq!(schedules.len(), 2);
        assert_eq!(
            schedules[0].id(),
            &ImportedScheduleId::from("SVC1".to_owned())
        );
        assert_eq!(schedules[0].dates(), &["20260501"]);
        assert_eq!(
            schedules[1].id(),
            &ImportedScheduleId::from("SVC2".to_owned())
        );
        let mut dates2 = schedules[1].dates().to_vec();
        dates2.sort();
        assert_eq!(dates2, vec!["20260502", "20260509"]);
    }

    // ── stations (additional) ────────────────────────────────────────────────

    #[test]
    fn station_with_no_child_stops_is_still_included() {
        let parser = StubParser {
            stops: vec![station("S1", "Paris Nord")],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let result = importer.stations();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), &ImportedStationId::from("S1".to_owned()));
    }

    #[test]
    fn station_lat_lon_are_propagated() {
        let parser = StubParser {
            stops: vec![GTFSStop::new(
                sid("S1"),
                "Marseille Saint-Charles".to_owned(),
                43.3026,
                5.3800,
                GTFSLocationType::Station,
                None,
            )],
            stop_times: vec![],
            trips: vec![],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        let result = importer.stations();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            ImportedStation::new(
                ImportedStationId::from("S1".to_owned()),
                "Marseille Saint-Charles".to_owned(),
                43.3026,
                5.3800,
            )
        );
    }

    // ── trips (additional) ───────────────────────────────────────────────────

    #[test]
    fn single_stop_trip_produces_no_legs() {
        let parser = StubParser {
            stops: vec![station("S1", "Paris Nord"), stop("S1-A", "S1")],
            stop_times: vec![stop_time("T1", "S1-A", 0, 800, 0)],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        assert!(importer.trip_legs().is_empty());
    }

    #[test]
    fn trip_id_absent_from_trips_table_is_dropped() {
        // stop_times references "T1" but the trips table is empty.
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                station("S2", "Lyon Perrache"),
                stop("S2-A", "S2"),
            ],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "S2-A", 1200, 0, 1),
            ],
            trips: vec![],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        assert!(importer.trip_legs().is_empty());
    }

    #[test]
    fn leg_where_both_stops_belong_to_same_station_is_dropped() {
        // Two platform stops of the same station appear in one trip;
        // the reconciled leg would have origin == destination, which carries
        // no domain meaning, so it must be filtered out.
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                stop("S1-B", "S1"),
            ],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "S1-B", 900, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");
        assert!(importer.trip_legs().is_empty());
    }

    // ── integration ──────────────────────────────────────────────────────────

    #[test]
    fn full_pipeline_integration() {
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                station("S2", "Lyon Perrache"),
                stop("S2-A", "S2"),
            ],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "S2-A", 1200, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar_dates: vec![GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_owned()),
                "20260601".to_owned(),
                GTFSExceptionType::ServiceAdded,
            )],
        };
        let importer = GTFSImporter::from_parser(&parser, "source");

        // stations
        let mut stations = importer.stations().to_vec();
        stations.sort_by_key(|s| s.id().clone());
        assert_eq!(stations.len(), 2);
        assert_eq!(stations[0].id(), &ImportedStationId::from("S1".to_owned()));
        assert_eq!(stations[1].id(), &ImportedStationId::from("S2".to_owned()));

        // trip legs
        let legs = importer.trip_legs();
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].origin(), &ImportedStationId::from("S1".to_owned()));
        assert_eq!(
            legs[0].destination(),
            &ImportedStationId::from("S2".to_owned())
        );
        assert_eq!(legs[0].departure(), 800);
        assert_eq!(legs[0].arrival(), 1200);

        // schedules
        let schedules = importer.schedules();
        assert_eq!(schedules.len(), 1);
        assert_eq!(
            schedules[0].id(),
            &ImportedScheduleId::from("SVC1".to_owned())
        );
        assert_eq!(schedules[0].dates(), &["20260601"]);

        // schedules_by_route
        let by_route = importer.schedules_by_route();
        let r1 = ImportedRouteId::from("R1".to_owned());
        assert_eq!(
            by_route[&r1],
            vec![ImportedScheduleId::from("SVC1".to_owned())]
        );
    }
}
