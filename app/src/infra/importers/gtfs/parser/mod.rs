use derive_more::{Constructor, From};

use crate::infra::importers::gtfs::parser::{stations::GTFSStationParser, trips::GTFSTripsParser};

mod stations;
mod trips;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

/// A station can contain several, possibly abstract, stops. For example `StationId` "Paris Gare de
/// Lyon" can contain `StopId`s "Paris Gare de Lyon - TGV" and "Paris Gare de Lyon - OUIGO" amongst
/// others.
#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct ImportedStopId(String);

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct ImportedStation {
    id: ImportedStationId,
    name: String,
    lat: f64,
    lon: f64,
    stops: Vec<ImportedStopId>,
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTrip {
    origin: ImportedStopId,
    destination: ImportedStopId,
    departure: usize,
    arrival: usize,
}

pub struct GTFSParser {
    stations: Vec<ImportedStation>,
    trips: Vec<ImportedTrip>,
}

impl GTFSParser {
    pub fn parse(location: &str) -> Option<Self> {
        let read = |filename: &str| -> Option<String> {
            std::fs::read_to_string(format!("{location}/{filename}")).ok()
        };

        let stops = read("stops.txt")?;
        let trips_file = read("trips.txt")?;
        let calendar_dates = read("calendar_dates.txt")?;
        let stop_times = read("stop_times.txt")?;

        let stations = GTFSStationParser::from(stops).stations()?;
        let trips = GTFSTripsParser::new(trips_file, calendar_dates, stop_times).trips();

        Some(Self { stations, trips })
    }

    pub fn trips(&self) -> &[ImportedTrip] {
        &self.trips
    }

    pub fn stations(&self) -> &[ImportedStation] {
        &self.stations
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
    use chrono_tz::Europe::Paris;
    use pretty_assertions::assert_eq;
    use std::fs;

    use super::*;

    fn write_gtfs_fixture(dir: &std::path::Path) {
        fs::write(
            dir.join("stops.txt"),
            "stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station\n\
             StopArea:PARIS,Paris Gare de Lyon,,48.8448,2.3735,,,1,\n\
             StopPoint:PARIS_TGV,Paris Gare de Lyon TGV,,48.8448,2.3735,,,0,StopArea:PARIS\n\
             StopArea:LYON,Lyon Part-Dieu,,45.7605,4.8597,,,1,\n\
             StopPoint:LYON_MAIN,Lyon Part-Dieu Main,,45.7605,4.8597,,,0,StopArea:LYON",
        )
        .unwrap();

        fs::write(
            dir.join("trips.txt"),
            "route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id\n\
             ROUTE1,SVC1,TRIP1,Lyon Part-Dieu,0,,",
        )
        .unwrap();

        fs::write(
            dir.join("calendar_dates.txt"),
            "service_id,date,exception_type\n\
             SVC1,20260225,1",
        )
        .unwrap();

        fs::write(
            dir.join("stop_times.txt"),
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled\n\
             TRIP1,10:00:00,10:00:00,StopPoint:PARIS_TGV,0,,0,1,\n\
             TRIP1,12:00:00,12:00:00,StopPoint:LYON_MAIN,1,,1,0,",
        )
        .unwrap();
    }

    fn paris_timestamp(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> usize {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(y, mo, d).unwrap(),
            NaiveTime::from_hms_opt(h, mi, s).unwrap(),
        );
        Paris.from_local_datetime(&dt).single().unwrap().timestamp() as usize
    }

    #[test]
    fn test_parse_returns_stations_and_trips() {
        let dir = std::env::temp_dir().join("gtfs_parser_test");
        fs::create_dir_all(&dir).unwrap();
        write_gtfs_fixture(&dir);

        let parser = GTFSParser::parse(dir.to_str().unwrap()).expect("parse should succeed");

        let stations = parser.stations().to_vec();
        assert_eq!(
            stations,
            vec![
                ImportedStation::new(
                    ImportedStationId::from("StopArea:PARIS".to_string()),
                    "Paris Gare de Lyon".to_string(),
                    48.8448,
                    2.3735,
                    vec![ImportedStopId::from("StopPoint:PARIS_TGV".to_string())],
                ),
                ImportedStation::new(
                    ImportedStationId::from("StopArea:LYON".to_string()),
                    "Lyon Part-Dieu".to_string(),
                    45.7605,
                    4.8597,
                    vec![ImportedStopId::from("StopPoint:LYON_MAIN".to_string())],
                ),
            ]
        );

        let mut trips = parser.trips().to_vec();
        trips.sort();
        assert_eq!(
            trips,
            vec![ImportedTrip::new(
                ImportedStopId::from("StopPoint:PARIS_TGV".to_string()),
                ImportedStopId::from("StopPoint:LYON_MAIN".to_string()),
                paris_timestamp(2026, 2, 25, 10, 0, 0),
                paris_timestamp(2026, 2, 25, 12, 0, 0),
            )]
        );
    }

    #[test]
    fn test_parse_returns_none_when_directory_missing() {
        let result = GTFSParser::parse("/nonexistent/path/to/gtfs");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_returns_none_when_stops_file_missing() {
        let dir = std::env::temp_dir().join("gtfs_parser_test_no_stops");
        fs::create_dir_all(&dir).unwrap();
        // Write all files except stops.txt
        fs::write(dir.join("trips.txt"), "route_id,service_id,trip_id\n").unwrap();
        fs::write(
            dir.join("calendar_dates.txt"),
            "service_id,date,exception_type\n",
        )
        .unwrap();
        fs::write(
            dir.join("stop_times.txt"),
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n",
        )
        .unwrap();

        let result = GTFSParser::parse(dir.to_str().unwrap());
        assert!(result.is_none());
    }
}
