use std::{collections::HashMap, hash::Hash};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::Europe::Paris;

use derive_more::{Constructor, From};

use crate::infra::importers::gtfs::{GTFSStopId, parser::GTFSTrip};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
struct TripId(String);

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
struct ServiceId(String);

#[derive(Debug, Clone, Constructor, PartialEq)]
struct Stop {
    id: GTFSStopId,
    trip: TripId,
    arrival: String,
    departure: String,
    order: usize,
}

#[derive(Debug, Clone, Constructor, PartialEq)]
struct TripsHeader {
    trip_id: usize,
    service_id: usize,
}

#[derive(Debug, Clone, Constructor, PartialEq)]
struct CalendarDatesHeader {
    service_id: usize,
    date: usize,
    exception: usize,
}

#[derive(Debug, Clone, Constructor, PartialEq)]
struct StopTimesHeader {
    trip_id: usize,
    arrival_time: usize,
    departure_time: usize,
    stop_id: usize,
    stop_sequence: usize,
}

#[derive(Debug, Clone)]
pub struct GTFSTripsParser {
    trips: String,
    calendar_dates: String,
    stop_times: String,
}

impl GTFSTripsParser {
    pub fn new(
        trips_file_content: String,
        calendar_dates_file_content: String,
        stop_times_content: String,
    ) -> Self {
        Self {
            trips: trips_file_content,
            calendar_dates: calendar_dates_file_content,
            stop_times: stop_times_content,
        }
    }

    pub fn trips(&self) -> Vec<GTFSTrip> {
        let Some(header) = self.stop_times_header() else {
            return vec![];
        };
        let mut rows = self.stop_times.split('\n');
        let _ = rows.next();

        // Groupe stops by trip/route
        let mut stops_by_trip: HashMap<TripId, Vec<Stop>> = HashMap::new();
        for row in rows {
            let cols: Vec<&str> = row.split(",").collect();
            let (Some(trip_id), Some(arrival), Some(departure), Some(stop_id), Some(sequence)) = (
                cols.get(header.trip_id)
                    .map(|id| TripId::from(id.to_string())),
                cols.get(header.arrival_time).map(|date| date.to_string()),
                cols.get(header.departure_time).map(|date| date.to_string()),
                cols.get(header.stop_id)
                    .map(|id| GTFSStopId::from(id.to_string())),
                cols.get(header.stop_sequence)
                    .and_then(|val| val.parse::<usize>().ok()),
            ) else {
                continue;
            };

            match stops_by_trip.get_mut(&trip_id) {
                Some(values) => values.push(Stop {
                    id: stop_id,
                    trip: trip_id,
                    arrival,
                    departure,
                    order: sequence,
                }),
                None => {
                    stops_by_trip.insert(
                        trip_id.clone(),
                        vec![Stop {
                            id: stop_id,
                            trip: trip_id,
                            arrival,
                            departure,
                            order: sequence,
                        }],
                    );
                }
            };
        }

        let (Some(services_by_trip), Some(dates_by_service)) =
            (self.services_by_trip(), self.dates_by_service())
        else {
            return vec![];
        };

        // For each trip/route, compute all stop combinations
        let mut trips = vec![];
        for (trip, stops) in stops_by_trip.iter() {
            let Some(dates) = services_by_trip.get(trip).map(|services| {
                services
                    .iter()
                    .filter_map(|service_id| dates_by_service.get(service_id).cloned())
                    .flatten()
                    .collect::<Vec<String>>()
            }) else {
                continue;
            };
            for (idx, origin) in stops.iter().enumerate() {
                for destination in stops[idx + 1..].iter() {
                    for date in dates.iter() {
                        let (Some(departure), Some(arrival)) = (
                            Self::parse_timestamp(date, &origin.departure),
                            Self::parse_timestamp(date, &destination.arrival),
                        ) else {
                            continue;
                        };
                        trips.push(GTFSTrip {
                            origin: origin.id.clone(),
                            destination: destination.id.clone(),
                            departure,
                            arrival,
                        });
                    }
                }
            }
        }

        trips
    }

    fn parse_timestamp(date: &str, time: &str) -> Option<usize> {
        let naive_date = NaiveDate::parse_from_str(date.trim(), "%Y%m%d").ok()?;
        let naive_time = NaiveTime::parse_from_str(time.trim(), "%H:%M:%S").ok()?;
        let naive_dt = NaiveDateTime::new(naive_date, naive_time);
        // TODO: tz should be found in agency.txt
        let dt = Paris.from_local_datetime(&naive_dt).single()?;
        Some(dt.timestamp() as usize)
    }

    fn services_by_trip(&self) -> Option<HashMap<TripId, Vec<ServiceId>>> {
        let header = self.trips_header()?;
        let mut rows = self.trips.split('\n');
        let _ = rows.next();

        let mut map: HashMap<TripId, Vec<ServiceId>> = HashMap::new();
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(trip_id), Some(service_id)) = (
                cols.get(header.trip_id)
                    .map(|id| TripId::from(id.to_string())),
                cols.get(header.service_id)
                    .map(|id| ServiceId::from(id.to_string())),
            ) else {
                continue;
            };

            match map.get_mut(&trip_id) {
                Some(values) => values.push(service_id),
                None => {
                    map.insert(trip_id.clone(), vec![service_id]);
                }
            }
        }

        Some(map)
    }

    fn dates_by_service(&self) -> Option<HashMap<ServiceId, Vec<String>>> {
        let header = self.calendar_dates_header()?;
        let mut rows = self.calendar_dates.split('\n');
        let _ = rows.next();

        let mut map: HashMap<ServiceId, Vec<String>> = HashMap::new();
        for row in rows {
            let cols: Vec<&str> = row.split(",").collect();
            let (Some(service_id), Some(date), Some(exception_type)) = (
                cols.get(header.service_id)
                    .map(|id| ServiceId::from(id.to_string())),
                cols.get(header.date).map(|date| date.to_string()),
                cols.get(header.exception)
                    .and_then(|val| val.parse::<usize>().ok()),
            ) else {
                continue;
            };

            if exception_type == 1 {
                match map.get_mut(&service_id) {
                    Some(values) => values.push(date),
                    None => {
                        map.insert(service_id, vec![date]);
                    }
                }
            }
        }

        Some(map)
    }

    fn trips_header(&self) -> Option<TripsHeader> {
        let first_row = self.trips.split("\n").next()?;
        let mut trip_id = None;
        let mut service_id = None;

        for (idx, w) in first_row.split(",").enumerate() {
            match w {
                "trip_id" => trip_id = Some(idx),
                "service_id" => service_id = Some(idx),
                _ => {}
            }
        }

        if trip_id.is_some() && service_id.is_some() {
            return Some(TripsHeader {
                trip_id: trip_id.unwrap(),
                service_id: service_id.unwrap(),
            });
        }

        None
    }

    fn calendar_dates_header(&self) -> Option<CalendarDatesHeader> {
        let first_row = self.calendar_dates.split("\n").next()?;
        let mut service_id = None;
        let mut date = None;
        let mut exception_type = None;

        for (idx, w) in first_row.split(",").enumerate() {
            match w {
                "service_id" => service_id = Some(idx),
                "date" => date = Some(idx),
                "exception_type" => exception_type = Some(idx),
                _ => {}
            }
        }

        if service_id.is_some() && date.is_some() && exception_type.is_some() {
            return Some(CalendarDatesHeader {
                service_id: service_id.unwrap(),
                date: date.unwrap(),
                exception: exception_type.unwrap(),
            });
        }

        None
    }

    fn stop_times_header(&self) -> Option<StopTimesHeader> {
        let first_row = self.stop_times.split("\n").next()?;
        let mut trip_id = None;
        let mut arrival_time = None;
        let mut departure_time = None;
        let mut stop_id = None;
        let mut stop_sequence = None;

        for (idx, w) in first_row.split(",").enumerate() {
            match w {
                "trip_id" => trip_id = Some(idx),
                "arrival_time" => arrival_time = Some(idx),
                "departure_time" => departure_time = Some(idx),
                "stop_id" => stop_id = Some(idx),
                "stop_sequence" => stop_sequence = Some(idx),
                _ => {}
            }
        }

        if trip_id.is_some()
            && arrival_time.is_some()
            && departure_time.is_some()
            && stop_id.is_some()
            && stop_sequence.is_some()
        {
            return Some(StopTimesHeader {
                trip_id: trip_id.unwrap(),
                arrival_time: arrival_time.unwrap(),
                departure_time: departure_time.unwrap(),
                stop_id: stop_id.unwrap(),
                stop_sequence: stop_sequence.unwrap(),
            });
        }

        None
    }
}

#[cfg(test)]
mod test_gtfs_trips_parser {
    use pretty_assertions::assert_eq;

    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Minimal valid trips / calendar_dates / stop_times fixtures (one trip,
    /// two stops, one active date) reused across several tests.
    fn valid_trips() -> &'static str {
        "route_id,service_id,trip_id\n,SVC1,TRIP1"
    }

    fn valid_calendar_dates() -> &'static str {
        "service_id,date,exception_type\nSVC1,20260501,1"
    }

    fn valid_stop_times_two_stops() -> &'static str {
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
         TRIP1,09:00:00,09:00:00,STOP_A,0\n\
         TRIP1,09:10:00,09:10:00,STOP_B,1"
    }

    fn valid_stop_times_one_stop() -> &'static str {
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
         TRIP1,09:00:00,09:00:00,STOP_A,0"
    }

    // ── happy path ───────────────────────────────────────────────────────────

    #[test]
    fn test_parse_ok() {
        let parser = GTFSTripsParser::new(
            "route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id
FR:Line::B10C45A0-C32C-4232-85F2-4BB81B810084:,000071,OCESN117756F1187_F:TER:FR:Line::B10C45A0-C32C-4232-85F2-4BB81B810084::87723197:87713040:10:1112:20260714,117756,1,105,".to_string(),
            "service_id,date,exception_type
000071,20260501,1
000071,20260508,1".to_string(),
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
OCESN117756F1187_F:TER:FR:Line::B10C45A0-C32C-4232-85F2-4BB81B810084::87723197:87713040:10:1112:20260714,09:16:00,09:16:00,StopPoint:OCETrain TER-87723197,0,,0,1,
OCESN117756F1187_F:TER:FR:Line::B10C45A0-C32C-4232-85F2-4BB81B810084::87723197:87713040:10:1112:20260714,09:28:00,09:30:00,StopPoint:OCETrain TER-87721282,1,,0,0,
OCESN117756F1187_F:TER:FR:Line::B10C45A0-C32C-4232-85F2-4BB81B810084::87723197:87713040:10:1112:20260714,09:38:00,09:39:00,StopPoint:OCETrain TER-87721332,2,,0,0,".to_string());

        let mut trips = parser.trips();

        let mut expected_trips = vec![
            // A -> B, 20260501
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87723197".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721282".to_string()),
                1777619760, // 9h16
                1777620480, // 9h28
            ),
            // B -> C, 20260501
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87721282".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721332".to_string()),
                1777620600, // 9h30
                1777621080, // 9h38
            ),
            // A -> C, 20260501
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87723197".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721332".to_string()),
                1777619760, // 9h16
                1777621080, // 9h38
            ),
            // A -> B, 20260508
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87723197".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721282".to_string()),
                1778224560, // 9h16
                1778225280, // 9h28
            ),
            // B -> C, 20260508
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87721282".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721332".to_string()),
                1778225400, // 9h30
                1778225880, // 9h38
            ),
            // A -> C, 20260508
            GTFSTrip::new(
                GTFSStopId::from("StopPoint:OCETrain TER-87723197".to_string()),
                GTFSStopId::from("StopPoint:OCETrain TER-87721332".to_string()),
                1778224560, // 9h16
                1778225880, // 9h38
            ),
        ];
        expected_trips.sort();
        trips.sort();
        assert_eq!(trips, expected_trips)
    }

    // ── fully missing data ───────────────────────────────────────────────────

    #[test]
    fn test_all_files_empty() {
        let parser = GTFSTripsParser::new(String::new(), String::new(), String::new());
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_stop_times_empty() {
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            String::new(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_trips_file_empty() {
        // services_by_trip() returns None → trips() returns empty vec
        let parser = GTFSTripsParser::new(
            String::new(),
            valid_calendar_dates().to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_calendar_dates_file_empty() {
        // dates_by_service() returns None → trips() returns empty vec
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            String::new(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    // ── missing required header columns ──────────────────────────────────────

    #[test]
    fn test_stop_times_missing_required_column() {
        // stop_sequence column is absent → stop_times_header() returns None
        let stop_times_bad_header =
            "trip_id,arrival_time,departure_time,stop_id\nTRIP1,09:00:00,09:00:00,STOP_A";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            stop_times_bad_header.to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_trips_file_missing_required_column() {
        // trip_id column is absent → trips_header() / services_by_trip() returns None
        let trips_bad_header = "route_id,service_id\n,SVC1";
        let parser = GTFSTripsParser::new(
            trips_bad_header.to_string(),
            valid_calendar_dates().to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_calendar_dates_missing_required_column() {
        // exception_type column is absent → calendar_dates_header() / dates_by_service() returns None
        let calendar_bad_header = "service_id,date\nSVC1,20260501";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            calendar_bad_header.to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    // ── partially missing / malformed row data ────────────────────────────────

    #[test]
    fn test_stop_times_rows_with_malformed_sequence() {
        // One row has a non-numeric stop_sequence → that row is skipped, valid rows kept
        let stop_times = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                          TRIP1,09:00:00,09:00:00,STOP_A,0\n\
                          TRIP1,09:10:00,09:10:00,STOP_B,NOT_A_NUMBER\n\
                          TRIP1,09:20:00,09:20:00,STOP_C,2";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            stop_times.to_string(),
        );
        // Only STOP_A and STOP_C are valid → one trip pair expected
        let trips = parser.trips();
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].origin, GTFSStopId::from("STOP_A".to_string()));
        assert_eq!(trips[0].destination, GTFSStopId::from("STOP_C".to_string()));
    }

    #[test]
    fn test_stop_times_rows_with_missing_columns() {
        // One row is truncated → skipped; remaining valid rows produce trips
        let stop_times = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                          TRIP1,09:00:00,09:00:00,STOP_A,0\n\
                          TRIP1,09:10:00\n\
                          TRIP1,09:20:00,09:20:00,STOP_C,2";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            stop_times.to_string(),
        );
        let trips = parser.trips();
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].origin, GTFSStopId::from("STOP_A".to_string()));
        assert_eq!(trips[0].destination, GTFSStopId::from("STOP_C".to_string()));
    }

    #[test]
    fn test_invalid_time_format_skips_trip() {
        // The departure time is not parseable → parse_timestamp returns None → trip skipped
        let stop_times = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                          TRIP1,BADTIME,BADTIME,STOP_A,0\n\
                          TRIP1,09:10:00,09:10:00,STOP_B,1";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            stop_times.to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    // ── no matching service / dates ───────────────────────────────────────────

    #[test]
    fn test_no_matching_service_for_trip() {
        // The stop_times reference TRIP1, but the trips file maps TRIP1 to a
        // different service that has no dates → no trips produced.
        let trips = "route_id,service_id,trip_id\n,SVC_UNKNOWN,TRIP1";
        let parser = GTFSTripsParser::new(
            trips.to_string(),
            valid_calendar_dates().to_string(), // only SVC1 has dates
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_no_matching_dates_for_service() {
        // The service exists in trips but has no entry in calendar_dates
        let calendar_dates_other_service = "service_id,date,exception_type\nOTHER_SVC,20260501,1";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            calendar_dates_other_service.to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    // ── exception_type filtering ──────────────────────────────────────────────

    #[test]
    fn test_exception_type_not_1_is_excluded() {
        // exception_type == 2 means service removed on that date; must be ignored
        let calendar_dates = "service_id,date,exception_type\nSVC1,20260501,2";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            calendar_dates.to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_mixed_exception_types_only_type_1_used() {
        // One added date (type=1) and one removed date (type=2) for the same service.
        // Only the type=1 date should produce trips.
        let calendar_dates = "service_id,date,exception_type\nSVC1,20260501,1\nSVC1,20260508,2";
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            calendar_dates.to_string(),
            valid_stop_times_two_stops().to_string(),
        );
        // Only one date active → one pair of stops → one trip
        assert_eq!(parser.trips().len(), 1);
    }

    // ── edge cases ───────────────────────────────────────────────────────────

    #[test]
    fn test_single_stop_produces_no_trips() {
        // With only one stop there are no (origin, destination) pairs
        let parser = GTFSTripsParser::new(
            valid_trips().to_string(),
            valid_calendar_dates().to_string(),
            valid_stop_times_one_stop().to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }

    #[test]
    fn test_header_only_no_data_rows() {
        // Files have valid headers but zero data rows → empty result, no panic
        let parser = GTFSTripsParser::new(
            "route_id,service_id,trip_id".to_string(),
            "service_id,date,exception_type".to_string(),
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence".to_string(),
        );
        assert_eq!(parser.trips(), vec![]);
    }
}
