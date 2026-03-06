use crate::infra::importers::gtfs::{
    GTFSCalendar, GTFSCalendarDate, GTFSRoute, GTFSStop, GTFSStopTime, GTFSTrip, ParseGTFS,
};

use self::{
    calendar::CalendarParser, calendar_dates::CalendarDatesParser, routes::RoutesParser,
    stop_times::StopTimesParser, stops::StopsParser, trips::TripsFileParser,
};

pub mod calendar;
pub mod calendar_dates;
pub mod routes;
pub mod stop_times;
pub mod stops;
pub mod trips;

#[derive(Debug)]
pub enum GTFSParseError {
    Io(std::io::Error),
    MissingColumn(String),
}

impl std::fmt::Display for GTFSParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GTFSParseError::Io(e) => write!(f, "I/O error reading GTFS file: {e}"),
            GTFSParseError::MissingColumn(col) => {
                write!(f, "Missing required CSV column: {col}")
            }
        }
    }
}

impl std::error::Error for GTFSParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GTFSParseError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for GTFSParseError {
    fn from(e: std::io::Error) -> Self {
        GTFSParseError::Io(e)
    }
}

#[derive(Debug)]
pub struct GTFSParser {
    stops: Vec<GTFSStop>,
    stop_times: Vec<GTFSStopTime>,
    trips: Vec<GTFSTrip>,
    /// Rows from `calendar.txt`. Empty when the feed does not include the file.
    calendar: Vec<GTFSCalendar>,
    calendar_dates: Vec<GTFSCalendarDate>,
    routes: Vec<GTFSRoute>,
}

impl GTFSParser {
    pub fn parse(location: &str) -> Result<Self, GTFSParseError> {
        let read = |filename: &str| -> Result<String, GTFSParseError> {
            std::fs::read_to_string(format!("{location}/{filename}")).map_err(GTFSParseError::Io)
        };

        let stops = StopsParser::from(read("stops.txt")?).parse()?;
        let trips = TripsFileParser::from(read("trips.txt")?).parse()?;
        let calendar_dates = CalendarDatesParser::from(read("calendar_dates.txt")?).parse()?;
        // calendar.txt is optional in GTFS: some feeds rely exclusively on
        // calendar_dates.txt (method 1). Treat a missing file as an empty list.
        let calendar = read("calendar.txt")
            .ok()
            .map(|c| CalendarParser::from(c).parse())
            .transpose()?
            .unwrap_or_default();
        let stop_times = StopTimesParser::from(read("stop_times.txt")?).parse()?;
        let routes = RoutesParser::from(read("routes.txt")?).parse()?;

        Ok(Self {
            stops,
            stop_times,
            trips,
            calendar,
            calendar_dates,
            routes,
        })
    }
}

impl ParseGTFS for GTFSParser {
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

#[cfg(test)]
mod tests {

    use pretty_assertions::assert_eq;
    use std::fs;

    use crate::infra::importers::gtfs::{
        GTFSCalendarDate, GTFSExceptionType, GTFSLocationType, GTFSRoute, GTFSRouteId,
        GTFSRouteType, GTFSServiceId, GTFSStop, GTFSStopId, GTFSStopTime, GTFSTrip, GTFSTripId,
    };

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

        fs::write(
            dir.join("routes.txt"),
            "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color\n\
             ROUTE1,1187,TGV,Paris - Lyon,,2,,0749FF,FFFFFF",
        )
        .unwrap();
    }

    #[test]
    fn test_parse_reads_rows_from_all_files() {
        let dir = std::env::temp_dir().join("gtfs_parser_test");
        fs::create_dir_all(&dir).unwrap();
        write_gtfs_fixture(&dir);

        let parser = GTFSParser::parse(dir.to_str().unwrap()).expect("parse should succeed");

        assert_eq!(
            parser.stops(),
            &[
                GTFSStop::new(
                    GTFSStopId::from("StopArea:PARIS".to_string()),
                    "Paris Gare de Lyon".to_string(),
                    48.8448,
                    2.3735,
                    GTFSLocationType::Station,
                    None,
                ),
                GTFSStop::new(
                    GTFSStopId::from("StopPoint:PARIS_TGV".to_string()),
                    "Paris Gare de Lyon TGV".to_string(),
                    48.8448,
                    2.3735,
                    GTFSLocationType::Stop,
                    Some(GTFSStopId::from("StopArea:PARIS".to_string())),
                ),
                GTFSStop::new(
                    GTFSStopId::from("StopArea:LYON".to_string()),
                    "Lyon Part-Dieu".to_string(),
                    45.7605,
                    4.8597,
                    GTFSLocationType::Station,
                    None,
                ),
                GTFSStop::new(
                    GTFSStopId::from("StopPoint:LYON_MAIN".to_string()),
                    "Lyon Part-Dieu Main".to_string(),
                    45.7605,
                    4.8597,
                    GTFSLocationType::Stop,
                    Some(GTFSStopId::from("StopArea:LYON".to_string())),
                ),
            ]
        );

        assert_eq!(
            parser.trips(),
            &[GTFSTrip::new(
                GTFSTripId::from("TRIP1".to_string()),
                GTFSRouteId::from("ROUTE1".to_string()),
                GTFSServiceId::from("SVC1".to_string()),
            )]
        );

        assert_eq!(
            parser.calendar_dates(),
            &[GTFSCalendarDate::new(
                GTFSServiceId::from("SVC1".to_string()),
                "20260225".to_string(),
                GTFSExceptionType::ServiceAdded,
            )]
        );

        assert_eq!(
            parser.stop_times(),
            &[
                GTFSStopTime::new(
                    GTFSTripId::from("TRIP1".to_string()),
                    10 * 3600,
                    10 * 3600,
                    GTFSStopId::from("StopPoint:PARIS_TGV".to_string()),
                    0,
                ),
                GTFSStopTime::new(
                    GTFSTripId::from("TRIP1".to_string()),
                    12 * 3600,
                    12 * 3600,
                    GTFSStopId::from("StopPoint:LYON_MAIN".to_string()),
                    1,
                ),
            ]
        );

        assert_eq!(
            parser.routes(),
            &[GTFSRoute::new(
                GTFSRouteId::from("ROUTE1".to_string()),
                GTFSRouteType::Rail,
            )]
        );
    }

    #[test]
    fn test_parse_returns_none_when_directory_missing() {
        let result = GTFSParser::parse("/nonexistent/path/to/gtfs");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_io_error_is_io_variant() {
        let err = GTFSParser::parse("/nonexistent/path/to/gtfs").unwrap_err();
        assert!(matches!(err, GTFSParseError::Io(_)));
    }

    #[test]
    fn test_parse_io_error_display() {
        let err = GTFSParser::parse("/nonexistent/path/to/gtfs").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.starts_with("I/O error reading GTFS file:"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_parse_io_error_source_is_some() {
        use std::error::Error;
        let err = GTFSParser::parse("/nonexistent/path/to/gtfs").unwrap_err();
        assert!(err.source().is_some());
    }

    #[test]
    fn test_parse_missing_column_error_is_missing_column_variant() {
        // stops.txt has no required columns → MissingColumn propagated through parse()
        let dir = std::env::temp_dir().join("gtfs_parser_test_bad_stops_header");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("stops.txt"), "irrelevant_column\n").unwrap();
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

        let err = GTFSParser::parse(dir.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, GTFSParseError::MissingColumn(_)));
    }

    #[test]
    fn test_parse_missing_column_error_display() {
        let err = GTFSParseError::MissingColumn("stop_id".to_string());
        assert_eq!(err.to_string(), "Missing required CSV column: stop_id");
    }

    #[test]
    fn test_parse_missing_column_error_source_is_none() {
        use std::error::Error;
        let err = GTFSParseError::MissingColumn("stop_id".to_string());
        assert!(err.source().is_none());
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
        assert!(result.is_err());
    }
}
