use crate::infra::importers::gtfs::{
    GTFSLocationType, GTFSRawStop, GTFSStopId, parser::GTFSParseError,
};

struct StopsHeader {
    id: usize,
    name: usize,
    lat: usize,
    lon: usize,
    location_type: usize,
    parent_station: usize,
}

pub struct StopsParser {
    content: String,
}

impl From<String> for StopsParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl StopsParser {
    fn header(&self) -> Result<StopsHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut id = None;
        let mut name = None;
        let mut lat = None;
        let mut lon = None;
        let mut location_type = None;
        let mut parent_station = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col {
                "stop_id" => id = Some(idx),
                "stop_name" => name = Some(idx),
                "stop_lat" => lat = Some(idx),
                "stop_lon" => lon = Some(idx),
                "location_type" => location_type = Some(idx),
                "parent_station" => parent_station = Some(idx),
                _ => {}
            }
        }

        Ok(StopsHeader {
            id: id.ok_or_else(|| GTFSParseError::MissingColumn("stop_id".to_string()))?,
            name: name.ok_or_else(|| GTFSParseError::MissingColumn("stop_name".to_string()))?,
            lat: lat.ok_or_else(|| GTFSParseError::MissingColumn("stop_lat".to_string()))?,
            lon: lon.ok_or_else(|| GTFSParseError::MissingColumn("stop_lon".to_string()))?,
            location_type: location_type
                .ok_or_else(|| GTFSParseError::MissingColumn("location_type".to_string()))?,
            parent_station: parent_station
                .ok_or_else(|| GTFSParseError::MissingColumn("parent_station".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSRawStop>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut stops = vec![];
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(id), Some(name), Some(lat), Some(lon), Some(location_type), Some(parent)) = (
                cols.get(header.id),
                cols.get(header.name),
                cols.get(header.lat).and_then(|v| v.parse::<f64>().ok()),
                cols.get(header.lon).and_then(|v| v.parse::<f64>().ok()),
                cols.get(header.location_type)
                    .and_then(|v| GTFSLocationType::from_str(v)),
                cols.get(header.parent_station),
            ) else {
                continue;
            };

            let parent_station = if parent.is_empty() {
                None
            } else {
                Some(GTFSStopId::from(parent.to_string()))
            };

            stops.push(GTFSRawStop::new(
                GTFSStopId::from(id.to_string()),
                name.to_string(),
                lat,
                lon,
                location_type,
                parent_station,
            ));
        }

        Ok(stops)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn stop_id(id: &str) -> GTFSStopId {
        GTFSStopId::from(id.to_string())
    }

    fn raw_stop(
        id: &str,
        name: &str,
        lat: f64,
        lon: f64,
        location_type: GTFSLocationType,
        parent: Option<&str>,
    ) -> GTFSRawStop {
        GTFSRawStop::new(
            stop_id(id),
            name.to_string(),
            lat,
            lon,
            location_type,
            parent.map(|p| GTFSStopId::from(p.to_string())),
        )
    }

    // ── error paths ────────────────────────────────────────────────────────

    fn missing_col(result: Result<Vec<GTFSRawStop>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_stop_id_column() {
        let col = missing_col(
            StopsParser::from(
                "stop_name,stop_lat,stop_lon,location_type,parent_station\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_id");
    }

    #[test]
    fn missing_stop_name_column() {
        let col = missing_col(
            StopsParser::from(
                "stop_id,stop_lat,stop_lon,location_type,parent_station\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_name");
    }

    #[test]
    fn missing_stop_lat_column() {
        let col = missing_col(
            StopsParser::from(
                "stop_id,stop_name,stop_lon,location_type,parent_station\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_lat");
    }

    #[test]
    fn missing_stop_lon_column() {
        let col = missing_col(
            StopsParser::from(
                "stop_id,stop_name,stop_lat,location_type,parent_station\n".to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "stop_lon");
    }

    #[test]
    fn missing_location_type_column() {
        let col = missing_col(
            StopsParser::from("stop_id,stop_name,stop_lat,stop_lon,parent_station\n".to_string())
                .parse(),
        );
        assert_eq!(col, "location_type");
    }

    #[test]
    fn missing_parent_station_column() {
        let col = missing_col(
            StopsParser::from("stop_id,stop_name,stop_lat,stop_lon,location_type\n".to_string())
                .parse(),
        );
        assert_eq!(col, "parent_station");
    }

    #[test]
    fn empty_input_reports_missing_column() {
        assert!(matches!(
            StopsParser::from(String::new()).parse(),
            Err(GTFSParseError::MissingColumn(_))
        ));
    }

    // ── happy path ─────────────────────────────────────────────────────────

    #[test]
    fn header_only_yields_empty_vec() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn station_row_has_no_parent() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopArea:S1,Paris Nord,48.8448,2.3735,1,";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(
            result,
            vec![raw_stop(
                "StopArea:S1",
                "Paris Nord",
                48.8448,
                2.3735,
                GTFSLocationType::Station,
                None
            )]
        );
    }

    #[test]
    fn stop_row_has_parent() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopPoint:P1,Paris Nord TGV,48.8448,2.3735,0,StopArea:S1";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(
            result,
            vec![raw_stop(
                "StopPoint:P1",
                "Paris Nord TGV",
                48.8448,
                2.3735,
                GTFSLocationType::Stop,
                Some("StopArea:S1")
            )]
        );
    }

    #[test]
    fn rows_are_emitted_in_order_without_grouping() {
        // Both station and stop rows come out flat, order preserved.
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopArea:S1,Paris Nord,48.0,2.0,1,\n\
                       StopPoint:P1,Platform 1,48.0,2.0,0,StopArea:S1\n\
                       StopPoint:P2,Platform 2,48.1,2.1,0,StopArea:S1";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id(), &stop_id("StopArea:S1"));
        assert_eq!(result[0].location_type(), GTFSLocationType::Station);
        assert_eq!(result[0].parent_station(), None);
        assert_eq!(result[1].id(), &stop_id("StopPoint:P1"));
        assert_eq!(result[1].location_type(), GTFSLocationType::Stop);
        assert_eq!(result[1].parent_station(), Some(&stop_id("StopArea:S1")));
        assert_eq!(result[2].id(), &stop_id("StopPoint:P2"));
    }

    #[test]
    fn rows_with_extra_columns_are_parsed_correctly() {
        // Real GTFS files have many extra columns; they should be ignored.
        let content = "stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,location_type,parent_station\n\
                       StopArea:S1,Paris Nord,,48.8448,2.3735,,1,";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].lat(), 48.8448);
    }

    #[test]
    fn row_with_unparseable_lat_is_skipped() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopArea:S1,Paris Nord,NOT_A_FLOAT,2.3735,1,\n\
                       StopArea:S2,Lyon,45.7,4.8,1,";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), &stop_id("StopArea:S2"));
    }

    #[test]
    fn row_with_unknown_location_type_is_skipped() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopArea:S1,Paris Nord,48.0,2.0,99,\n\
                       StopArea:S2,Lyon,45.7,4.8,1,";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), &stop_id("StopArea:S2"));
    }

    #[test]
    fn empty_location_type_is_treated_as_stop() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       StopPoint:P1,Platform,48.0,2.0,,StopArea:S1";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location_type(), GTFSLocationType::Stop);
    }

    #[test]
    fn all_location_types_are_parsed() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n\
                       A,Stop,0.0,0.0,0,\n\
                       B,Station,0.0,0.0,1,\n\
                       C,Entrance,0.0,0.0,2,B\n\
                       D,Node,0.0,0.0,3,B\n\
                       E,Boarding,0.0,0.0,4,A";
        let result = StopsParser::from(content.to_string()).parse().unwrap();
        assert_eq!(result[0].location_type(), GTFSLocationType::Stop);
        assert_eq!(result[1].location_type(), GTFSLocationType::Station);
        assert_eq!(result[2].location_type(), GTFSLocationType::EntranceExit);
        assert_eq!(result[3].location_type(), GTFSLocationType::GenericNode);
        assert_eq!(result[4].location_type(), GTFSLocationType::BoardingArea);
    }
}
