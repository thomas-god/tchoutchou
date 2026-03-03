use crate::infra::importers::gtfs::{
    GTFSRouteId, GTFSServiceId, GTFSTrip, GTFSTripId, parsers::GTFSParseError,
};

struct TripsHeader {
    trip_id: usize,
    route_id: usize,
    service_id: usize,
}

pub struct TripsFileParser {
    content: String,
}

impl From<String> for TripsFileParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl TripsFileParser {
    fn header(&self) -> Result<TripsHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut trip_id = None;
        let mut route_id = None;
        let mut service_id = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col.trim() {
                "trip_id" => trip_id = Some(idx),
                "route_id" => route_id = Some(idx),
                "service_id" => service_id = Some(idx),
                _ => {}
            }
        }

        Ok(TripsHeader {
            trip_id: trip_id.ok_or_else(|| GTFSParseError::MissingColumn("trip_id".to_string()))?,
            route_id: route_id
                .ok_or_else(|| GTFSParseError::MissingColumn("route_id".to_string()))?,
            service_id: service_id
                .ok_or_else(|| GTFSParseError::MissingColumn("service_id".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSTrip>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut trips = vec![];
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(trip_id), Some(route_id), Some(service_id)) = (
                cols.get(header.trip_id)
                    .map(|v| GTFSTripId::from(v.to_string())),
                cols.get(header.route_id)
                    .map(|v| GTFSRouteId::from(v.to_string())),
                cols.get(header.service_id)
                    .map(|v| GTFSServiceId::from(v.to_string())),
            ) else {
                continue;
            };

            trips.push(GTFSTrip::new(trip_id, route_id, service_id));
        }

        Ok(trips)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn trip_id(id: &str) -> GTFSTripId {
        GTFSTripId::from(id.to_string())
    }
    fn route_id(id: &str) -> GTFSRouteId {
        GTFSRouteId::from(id.to_string())
    }
    fn svc(id: &str) -> GTFSServiceId {
        GTFSServiceId::from(id.to_string())
    }

    fn trip(trip: &str, route: &str, service: &str) -> GTFSTrip {
        GTFSTrip::new(trip_id(trip), route_id(route), svc(service))
    }

    // ── error paths ────────────────────────────────────────────────────────

    fn missing_col(result: Result<Vec<GTFSTrip>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_trip_id_column() {
        let col = missing_col(TripsFileParser::from("route_id,service_id\n".to_string()).parse());
        assert_eq!(col, "trip_id");
    }

    #[test]
    fn missing_route_id_column() {
        let col = missing_col(TripsFileParser::from("trip_id,service_id\n".to_string()).parse());
        assert_eq!(col, "route_id");
    }

    #[test]
    fn missing_service_id_column() {
        let col = missing_col(TripsFileParser::from("trip_id,route_id\n".to_string()).parse());
        assert_eq!(col, "service_id");
    }

    // ── happy path ─────────────────────────────────────────────────────────

    #[test]
    fn header_only_yields_empty_vec() {
        let content = "route_id,service_id,trip_id\n";
        let result = TripsFileParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn single_row_parsed_correctly() {
        let content = "route_id,service_id,trip_id\nROUTE1,SVC1,TRIP1";
        let result = TripsFileParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result, vec![trip("TRIP1", "ROUTE1", "SVC1")]);
    }

    #[test]
    fn multiple_rows_are_all_emitted() {
        let content =
            "route_id,service_id,trip_id\nROUTE1,SVC1,TRIP1\nROUTE1,SVC2,TRIP2\nROUTE2,SVC1,TRIP3";
        let result = TripsFileParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].trip_id(), &trip_id("TRIP1"));
        assert_eq!(result[1].service_id(), &svc("SVC2"));
        assert_eq!(result[2].route_id(), &route_id("ROUTE2"));
    }

    #[test]
    fn extra_columns_are_ignored() {
        // Real trips.txt has many extra columns (headsign, direction_id, etc.)
        let content = "route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id\n\
             ROUTE1,SVC1,TRIP1,Lyon Part-Dieu,0,,";
        let result = TripsFileParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].trip_id(), &trip_id("TRIP1"));
    }

    #[test]
    fn truncated_row_is_skipped() {
        let content = "route_id,service_id,trip_id\nROUTE1\nROUTE1,SVC1,TRIP1";
        let result = TripsFileParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].trip_id(), &trip_id("TRIP1"));
    }
}
