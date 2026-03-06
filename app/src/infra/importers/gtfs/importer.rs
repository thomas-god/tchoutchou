use std::collections::HashMap;

use derive_more::{Constructor, From};

use crate::app::schedule::{
    ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
    ImportedTripLeg, TrainDataToImport,
};

use super::{
    GTFSCalendar, GTFSCalendarDate, GTFSExceptionType, GTFSLocationType, GTFSRouteId,
    GTFSServiceId, GTFSStop, GTFSStopId, GTFSStopTime, GTFSTrip, GTFSTripId, ParseGTFS,
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
        let schedules = build_imported_schedules(parser.calendar(), parser.calendar_dates());
        let schedules_by_route = build_imported_schedules_by_route(parser.trips());

        Self {
            stations,
            trips,
            schedules,
            schedules_by_route,
            source: source.to_owned(),
        }
    }

    pub fn as_data(self) -> TrainDataToImport {
        TrainDataToImport::new(
            self.stations,
            self.trips,
            self.schedules,
            self.schedules_by_route,
            self.source,
        )
    }
}

/// Groups flat stop rows into `GTFSStation` values.
///
/// Rows where `location_type == Station` become the station entries (holding
/// name and coordinates). Rows where `location_type == Stop` are attached to
/// their `parent_station`. Stops with no `parent_station` are treated as
/// standalone stations (with themselves as their only child stop) so that
/// trip legs referencing them are not silently dropped.
fn build_stations(stops: &[GTFSStop]) -> Vec<GTFSStation> {
    // Map: parent station id → child stop ids.
    let mut children: HashMap<&str, Vec<GTFSStopId>> = HashMap::new();
    let mut orphan_stops: Vec<&GTFSStop> = vec![];
    for stop in stops
        .iter()
        .filter(|s| s.location_type() == GTFSLocationType::Stop)
    {
        match stop.parent_station() {
            Some(parent) => {
                children
                    .entry(parent.as_str())
                    .or_default()
                    .push(stop.id().clone());
            }
            None => orphan_stops.push(stop),
        }
    }

    let mut stations: Vec<GTFSStation> = stops
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
        .collect();

    // Parentless stops become their own station so they remain reachable
    // during trip reconciliation.
    for stop in orphan_stops {
        stations.push(GTFSStation::new(
            GTFSStationId::from(stop.id().as_str().to_owned()),
            stop.name().to_owned(),
            stop.lat(),
            stop.lon(),
            vec![stop.id().clone()],
        ));
    }

    stations
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

/// Builds `ImportedSchedule` values from calendar and calendar-date rows.
///
/// Supports both GTFS scheduling methods:
///
/// - **Method 1** (calendar-dates only): `ServiceAdded` rows in
///   `calendar_dates.txt` enumerate every date the service runs. A
///   corresponding `ServiceRemoved` row by itself (with no `calendar.txt`
///   entry for the same service) is a no-op.
///
/// - **Method 2** (calendar + calendar-dates): Each row in `calendar.txt`
///   defines a weekly frequency and date range that is first expanded into
///   the full set of matching calendar dates. `ServiceAdded` exceptions then
///   inject additional dates, and `ServiceRemoved` exceptions cancel them.
///
/// Services that end up with no dates after applying all rules are omitted
/// from the output.
fn build_imported_schedules(
    calendars: &[GTFSCalendar],
    calendar_dates: &[GTFSCalendarDate],
) -> Vec<ImportedSchedule> {
    use chrono::{Datelike, Duration, NaiveDate, Weekday};
    use std::collections::HashSet;

    let mut by_service: HashMap<GTFSServiceId, HashSet<String>> = HashMap::new();

    // Step 1: expand weekly calendar.txt entries into explicit date sets.
    for cal in calendars {
        let (Some(start), Some(end)) = (
            NaiveDate::parse_from_str(cal.start_date(), "%Y%m%d").ok(),
            NaiveDate::parse_from_str(cal.end_date(), "%Y%m%d").ok(),
        ) else {
            continue;
        };

        let enabled: [(Weekday, bool); 7] = [
            (Weekday::Mon, cal.monday()),
            (Weekday::Tue, cal.tuesday()),
            (Weekday::Wed, cal.wednesday()),
            (Weekday::Thu, cal.thursday()),
            (Weekday::Fri, cal.friday()),
            (Weekday::Sat, cal.saturday()),
            (Weekday::Sun, cal.sunday()),
        ];

        let dates = by_service.entry(cal.service_id().clone()).or_default();
        let mut current = start;
        while current <= end {
            if enabled
                .iter()
                .any(|(wd, on)| *on && *wd == current.weekday())
            {
                dates.insert(current.format("%Y%m%d").to_string());
            }
            current += Duration::days(1);
        }
    }

    // Step 2: apply calendar_dates.txt exceptions.
    for date in calendar_dates {
        match date.exception_type() {
            GTFSExceptionType::ServiceAdded => {
                by_service
                    .entry(date.service_id().clone())
                    .or_default()
                    .insert(date.date().to_owned());
            }
            GTFSExceptionType::ServiceRemoved => {
                if let Some(dates) = by_service.get_mut(date.service_id()) {
                    dates.remove(date.date());
                }
            }
        }
    }

    // Step 3: convert to domain type, dropping services with no remaining dates.
    by_service
        .into_iter()
        .filter(|(_, dates)| !dates.is_empty())
        .map(|(id, dates)| {
            let mut dates_vec: Vec<String> = dates.into_iter().collect();
            dates_vec.sort();
            ImportedSchedule::new(ImportedScheduleId::from(id.as_str().to_owned()), dates_vec)
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

    use super::*;
    use crate::infra::importers::gtfs::GTFSRoute;

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
        calendar: Vec<GTFSCalendar>,
        calendar_dates: Vec<GTFSCalendarDate>,
        routes: Vec<GTFSRoute>,
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
        fn calendar(&self) -> &[GTFSCalendar] {
            &self.calendar
        }
        fn calendar_dates(&self) -> &[GTFSCalendarDate] {
            &self.calendar_dates
        }
        fn routes(&self) -> &[GTFSRoute] {
            &self.routes
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut result = data.stations().to_vec();
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
    fn stop_rows_without_parent_are_exposed_as_standalone_stations() {
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut result = data.stations().to_vec();
        result.sort_by_key(|s| s.id().clone());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id(), &ImportedStationId::from("S1".to_owned()));
        assert_eq!(
            result[1].id(),
            &ImportedStationId::from("orphan".to_owned())
        );
    }

    #[test]
    fn orphan_stop_is_reachable_in_trips() {
        // An orphan stop (no parent_station) that appears in stop_times should
        // produce a valid trip leg, not be silently dropped.
        let parser = StubParser {
            stops: vec![
                station("S1", "Paris Nord"),
                stop("S1-A", "S1"),
                GTFSStop::new(
                    sid("orphan"),
                    "Orphan".to_owned(),
                    0.0,
                    0.0,
                    GTFSLocationType::Stop,
                    None,
                ),
            ],
            stop_times: vec![
                stop_time("T1", "S1-A", 0, 800, 0),
                stop_time("T1", "orphan", 1200, 0, 1),
            ],
            trips: vec![trip("T1", "R1", "SVC1")],
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let result = data.trip_legs();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].origin(),
            &ImportedStationId::from("S1".to_owned())
        );
        assert_eq!(
            result[0].destination(),
            &ImportedStationId::from("orphan".to_owned())
        );
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let result = data.trip_legs();

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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.trip_legs().is_empty());
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut result = data.trip_legs().to_vec();
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut result = data.trip_legs().to_vec();
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
            calendar: vec![],
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
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let by_route = data.schedules_by_route();

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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let by_route = data.schedules_by_route();

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
            calendar: vec![],
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
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.schedules().is_empty());
    }

    #[test]
    fn multiple_distinct_services_produce_multiple_schedules() {
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![],
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
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut schedules = data.schedules().to_vec();
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

    // ── schedules: calendar.txt (method 2) ──────────────────────────────────

    fn gtfs_cal(
        service_id: &str,
        mon: bool,
        tue: bool,
        wed: bool,
        thu: bool,
        fri: bool,
        sat: bool,
        sun: bool,
        start: &str,
        end: &str,
    ) -> GTFSCalendar {
        GTFSCalendar::new(
            GTFSServiceId::from(service_id.to_owned()),
            mon,
            tue,
            wed,
            thu,
            fri,
            sat,
            sun,
            start.to_owned(),
            end.to_owned(),
        )
    }

    #[test]
    fn weekly_calendar_expands_into_explicit_dates() {
        // 20260302 (Mon) → 20260306 (Fri): a Mon–Fri service should produce
        // exactly five dates, one per weekday.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, true, true, true, true, false, false, "20260302", "20260306",
            )],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

        assert_eq!(schedules.len(), 1);
        assert_eq!(
            schedules[0].id(),
            &ImportedScheduleId::from("SVC1".to_owned())
        );
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(
            dates,
            vec!["20260302", "20260303", "20260304", "20260305", "20260306"]
        );
    }

    #[test]
    fn calendar_service_added_injects_an_extra_date() {
        // Mon-only service over one week; a ServiceAdded on Wednesday should
        // add that date even though calendar.txt does not include Wednesdays.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, false, false, false, false, false, false, "20260302", "20260308",
            )],
            calendar_dates: vec![GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_owned()),
                "20260304".to_owned(), // Wednesday
                GTFSExceptionType::ServiceAdded,
            )],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

        assert_eq!(schedules.len(), 1);
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(dates, vec!["20260302", "20260304"]);
    }

    #[test]
    fn calendar_service_removed_cancels_a_date() {
        // Mon–Fri service; a ServiceRemoved on Wednesday cancels that day.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, true, true, true, true, false, false, "20260302", "20260306",
            )],
            calendar_dates: vec![GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_owned()),
                "20260304".to_owned(), // Wednesday
                GTFSExceptionType::ServiceRemoved,
            )],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

        assert_eq!(schedules.len(), 1);
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(dates, vec!["20260302", "20260303", "20260305", "20260306"]);
    }

    #[test]
    fn calendar_exception_only_affects_its_specific_date_not_same_weekday_in_other_weeks() {
        // Mon–Fri service over two weeks (Mon 2 Mar → Fri 13 Mar).
        // A ServiceRemoved for Wed 4 Mar must cancel only that date, leaving
        // Wed 11 Mar untouched, and a ServiceAdded for Sat 7 Mar (normally
        // outside the weekly pattern) must inject only that one Saturday.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, true, true, true, true, false, false, "20260302", "20260313",
            )],
            calendar_dates: vec![
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260304".to_owned(), // Wed week 1 — cancelled
                    GTFSExceptionType::ServiceRemoved,
                ),
                GTFSCalendarDate::new(
                    GTFSServiceId::from("SVC1".to_owned()),
                    "20260307".to_owned(), // Sat week 1 — extra date
                    GTFSExceptionType::ServiceAdded,
                ),
            ],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

        assert_eq!(schedules.len(), 1);
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(
            dates,
            vec![
                "20260302", // Mon w1
                "20260303", // Tue w1
                // Wed w1 (20260304) removed
                "20260305", // Thu w1
                "20260306", // Fri w1
                "20260307", // Sat w1 — injected by ServiceAdded
                "20260309", // Mon w2
                "20260310", // Tue w2
                "20260311", // Wed w2 — NOT affected by the w1 exception
                "20260312", // Thu w2
                "20260313", // Fri w2
            ]
        );
    }

    #[test]
    fn calendar_date_range_does_not_need_to_be_week_aligned() {
        // Range starts on a Wednesday and ends on a Tuesday, cutting across
        // the Mon–Sun boundary. A Mon–Fri service must include only the days
        // that are both in the pattern and within the range, with no phantom
        // dates from "completing" the partial weeks at either end.
        //   Wed 4 Mar → Tue 10 Mar:
        //     Wed 4  ✓  Thu 5  ✓  Fri 6  ✓  (Sat/Sun excluded)
        //     Mon 9  ✓  Tue 10 ✓
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, true, true, true, true, false, false, "20260304",
                "20260310", // Wed → Tue, not Mon-aligned
            )],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let schedules = data.schedules();

        assert_eq!(schedules.len(), 1);
        let mut dates = schedules[0].dates().to_vec();
        dates.sort();
        assert_eq!(
            dates,
            vec!["20260304", "20260305", "20260306", "20260309", "20260310"]
        );
    }

    #[test]
    fn calendar_all_dates_removed_produces_no_schedule() {
        // A service whose only calendar date is cancelled should be omitted.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![gtfs_cal(
                "SVC1", true, false, false, false, false, false, false, "20260302",
                "20260302", // single Monday
            )],
            calendar_dates: vec![GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_owned()),
                "20260302".to_owned(),
                GTFSExceptionType::ServiceRemoved,
            )],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.schedules().is_empty());
    }

    #[test]
    fn multiple_services_from_calendar_are_independent() {
        // Two services in calendar.txt with different weekly patterns should
        // each produce the correct distinct set of dates.
        let parser = StubParser {
            stops: vec![],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![
                // Weekday service
                gtfs_cal(
                    "WEEKDAY", true, true, true, true, true, false, false, "20260302", "20260306",
                ),
                // Weekend service
                gtfs_cal(
                    "WEEKEND", false, false, false, false, false, true, true, "20260302",
                    "20260308",
                ),
            ],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let mut schedules = data.schedules().to_vec();
        schedules.sort_by_key(|s| s.id().clone());

        assert_eq!(schedules.len(), 2);

        let weekday = schedules
            .iter()
            .find(|s| s.id().as_str() == "WEEKDAY")
            .unwrap();
        let mut weekday_dates = weekday.dates().to_vec();
        weekday_dates.sort();
        assert_eq!(
            weekday_dates,
            vec!["20260302", "20260303", "20260304", "20260305", "20260306"]
        );

        let weekend = schedules
            .iter()
            .find(|s| s.id().as_str() == "WEEKEND")
            .unwrap();
        let mut weekend_dates = weekend.dates().to_vec();
        weekend_dates.sort();
        assert_eq!(weekend_dates, vec!["20260307", "20260308"]);
    }

    // ── stations (additional) ────────────────────────────────────────────────

    #[test]
    fn station_with_no_child_stops_is_still_included() {
        let parser = StubParser {
            stops: vec![station("S1", "Paris Nord")],
            stop_times: vec![],
            trips: vec![],
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let result = data.stations();
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        let result = data.stations();
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.trip_legs().is_empty());
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.trip_legs().is_empty());
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
            calendar: vec![],
            calendar_dates: vec![],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();
        assert!(data.trip_legs().is_empty());
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
            calendar: vec![],
            calendar_dates: vec![GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_owned()),
                "20260601".to_owned(),
                GTFSExceptionType::ServiceAdded,
            )],
            routes: vec![],
        };
        let data = GTFSImporter::from_parser(&parser, "source").as_data();

        // stations
        let mut stations = data.stations().to_vec();
        stations.sort_by_key(|s| s.id().clone());
        assert_eq!(stations.len(), 2);
        assert_eq!(stations[0].id(), &ImportedStationId::from("S1".to_owned()));
        assert_eq!(stations[1].id(), &ImportedStationId::from("S2".to_owned()));

        // trip legs
        let legs = data.trip_legs();
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].origin(), &ImportedStationId::from("S1".to_owned()));
        assert_eq!(
            legs[0].destination(),
            &ImportedStationId::from("S2".to_owned())
        );
        assert_eq!(legs[0].departure(), 800);
        assert_eq!(legs[0].arrival(), 1200);

        // schedules
        let schedules = data.schedules();
        assert_eq!(schedules.len(), 1);
        assert_eq!(
            schedules[0].id(),
            &ImportedScheduleId::from("SVC1".to_owned())
        );
        assert_eq!(schedules[0].dates(), &["20260601"]);

        // schedules_by_route
        let by_route = data.schedules_by_route();
        let r1 = ImportedRouteId::from("R1".to_owned());
        assert_eq!(
            by_route[&r1],
            vec![ImportedScheduleId::from("SVC1".to_owned())]
        );
    }
}
