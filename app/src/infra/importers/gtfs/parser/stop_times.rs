use crate::infra::importers::gtfs::{GTFSRawStopTime, GTFSStopId, GTFSTripId, parser::GTFSParseError};

struct StopTimesHeader {
    trip_id: usize,
    arrival_time: usize,
    departure_time: usize,
    stop_id: usize,
    stop_sequence: usize,
}

pub struct StopTimesParser {
    content: String,
}

impl From<String> for StopTimesParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl StopTimesParser {
    fn header(&self) -> Result<StopTimesHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut trip_id = None;
        let mut arrival_time = None;
        let mut departure_time = None;
        let mut stop_id = None;
        let mut stop_sequence = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col {
                "trip_id" => trip_id = Some(idx),
                "arrival_time" => arrival_time = Some(idx),
                "departure_time" => departure_time = Some(idx),
                "stop_id" => stop_id = Some(idx),
                "stop_sequence" => stop_sequence = Some(idx),
                _ => {}
            }
        }

        Ok(StopTimesHeader {
            trip_id: trip_id
                .ok_or_else(|| GTFSParseError::MissingColumn("trip_id".to_string()))?,
            arrival_time: arrival_time
                .ok_or_else(|| GTFSParseError::MissingColumn("arrival_time".to_string()))?,
            departure_time: departure_time
                .ok_or_else(|| GTFSParseError::MissingColumn("departure_time".to_string()))?,
            stop_id: stop_id
                .ok_or_else(|| GTFSParseError::MissingColumn("stop_id".to_string()))?,
            stop_sequence: stop_sequence
                .ok_or_else(|| GTFSParseError::MissingColumn("stop_sequence".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSRawStopTime>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut stop_times = vec![];
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(trip_id), Some(arrival), Some(departure), Some(stop_id), Some(sequence)) = (
                cols.get(header.trip_id).map(|v| GTFSTripId::from(v.to_string())),
                cols.get(header.arrival_time).map(|v| parse_time(v)),
                cols.get(header.departure_time).map(|v| parse_time(v)),
                cols.get(header.stop_id).map(|v| GTFSStopId::from(v.to_string())),
                cols.get(header.stop_sequence)
                    .and_then(|v| v.parse::<usize>().ok()),
            ) else {
                continue;
            };

            stop_times.push(GTFSRawStopTime::new(trip_id, arrival, departure, stop_id, sequence));
        }

        Ok(stop_times)
    }
}

/// Parse `hh:mm:ss` into seconds from start of day.
/// GTFS allows hours ≥ 24 for trips running past midnight. Returns 0 for
/// malformed input.
fn parse_time(time: &str) -> usize {
    let parts: Vec<&str> = time.trim().split(':').collect();
    if parts.len() != 3 {
        return 0;
    }
    let (Ok(h), Ok(m), Ok(s)) = (
        parts[0].parse::<usize>(),
        parts[1].parse::<usize>(),
        parts[2].parse::<usize>(),
    ) else {
        return 0;
    };
    h * 3600 + m * 60 + s
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn trip_id(id: &str) -> GTFSTripId {
        GTFSTripId::from(id.to_string())
    }
    fn stop_id(id: &str) -> GTFSStopId {
        GTFSStopId::from(id.to_string())
    }

    // ── error paths ────────────────────────────────────────────────────────

    fn missing_col(result: Result<Vec<GTFSRawStopTime>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_trip_id_column() {
        let col = missing_col(
            StopTimesParser::from(
                "arrival_time,departure_time,stop_id,stop_sequence\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "trip_id");
    }

    #[test]
    fn missing_arrival_time_column() {
        let col = missing_col(
            StopTimesParser::from(
                "trip_id,departure_time,stop_id,stop_sequence\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "arrival_time");
    }

    #[test]
    fn missing_departure_time_column() {
        let col = missing_col(
            StopTimesParser::from(
                "trip_id,arrival_time,stop_id,stop_sequence\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "departure_time");
    }

    #[test]
    fn missing_stop_id_column() {
        let col = missing_col(
            StopTimesParser::from(
                "trip_id,arrival_time,departure_time,stop_sequence\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_id");
    }

    #[test]
    fn missing_stop_sequence_column() {
        let col = missing_col(
            StopTimesParser::from(
                "trip_id,arrival_time,departure_time,stop_id\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_sequence");
    }

    // ── happy path ─────────────────────────────────────────────────────────

    #[test]
    fn header_only_yields_empty_vec() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn single_row_parsed_correctly() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       TRIP1,10:00:00,10:00:00,STOP_A,0";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(
            result,
            vec![GTFSRawStopTime::new(trip_id("TRIP1"), 36000, 36000, stop_id("STOP_A"), 0)]
        );
    }

    #[test]
    fn times_are_seconds_from_midnight() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       T,09:16:30,09:17:00,S,1";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        let row = &result[0];
        assert_eq!(row.arrival(), 9 * 3600 + 16 * 60 + 30);
        assert_eq!(row.departure(), 9 * 3600 + 17 * 60);
    }

    #[test]
    fn past_midnight_time_is_handled() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       T,25:30:00,25:31:00,S,0";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result[0].arrival(), 25 * 3600 + 30 * 60);
    }

    #[test]
    fn multiple_rows_for_same_trip() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       TRIP1,09:00:00,09:00:00,STOP_A,0\n\
                       TRIP1,09:10:00,09:12:00,STOP_B,1\n\
                       TRIP1,09:20:00,09:20:00,STOP_C,2";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].stop_sequence(), 0);
        assert_eq!(result[1].stop_sequence(), 1);
        assert_eq!(result[2].stop_sequence(), 2);
    }

    #[test]
    fn row_with_non_numeric_sequence_is_skipped() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       TRIP1,09:00:00,09:00:00,STOP_A,0\n\
                       TRIP1,09:10:00,09:10:00,STOP_B,BAD\n\
                       TRIP1,09:20:00,09:20:00,STOP_C,2";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].stop_id(), &stop_id("STOP_A"));
        assert_eq!(result[1].stop_id(), &stop_id("STOP_C"));
    }

    #[test]
    fn truncated_row_is_skipped() {
        let content = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n\
                       TRIP1,09:00:00\n\
                       TRIP1,09:10:00,09:10:00,STOP_B,1";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].stop_id(), &stop_id("STOP_B"));
    }

    #[test]
    fn extra_columns_are_ignored() {
        let content =
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type\n\
             TRIP1,09:00:00,09:00:00,STOP_A,0,some_headsign,0";
        let result = StopTimesParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].trip_id(), &trip_id("TRIP1"));
    }
}
