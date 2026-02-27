use derive_more::Constructor;

use crate::infra::importers::gtfs::{GTFSStation, GTFSStationId, GTFSStopId};

use super::GTFSParseError;

#[derive(Debug, Clone, Constructor, PartialEq)]
struct GTFSHeaders {
    id: usize,
    name: usize,
    lat: usize,
    lon: usize,
    location_type: usize,
    parent_station: usize,
}

pub struct GTFSStationParser {
    content: String,
}

impl From<String> for GTFSStationParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl GTFSStationParser {
    fn headers(&self) -> Result<GTFSHeaders, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut id = None;
        let mut name = None;
        let mut lat = None;
        let mut lon = None;
        let mut location_type = None;
        let mut parent_station = None;

        for (idx, w) in first_row.split(',').enumerate() {
            match w {
                "stop_id" => id = Some(idx),
                "stop_name" => name = Some(idx),
                "stop_lat" => lat = Some(idx),
                "stop_lon" => lon = Some(idx),
                "location_type" => location_type = Some(idx),
                "parent_station" => parent_station = Some(idx),
                _ => {}
            }
        }

        Ok(GTFSHeaders {
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

    pub fn stations(&self) -> Result<Vec<GTFSStation>, GTFSParseError> {
        let headers = self.headers()?;

        let mut stations = vec![];
        let mut orphans = vec![];

        let mut rows = self.content.split("\n");
        let _ = rows.next();

        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (
                Some(id),
                Some(name),
                Some(lat),
                Some(lon),
                Some(location_type),
                Some(parent_station),
            ) = (
                cols.get(headers.id),
                cols.get(headers.name),
                cols.get(headers.lat)
                    .and_then(|lat| lat.parse::<f64>().ok()),
                cols.get(headers.lon)
                    .and_then(|lon| lon.parse::<f64>().ok()),
                cols.get(headers.location_type)
                    .and_then(|t| t.parse::<usize>().ok()),
                cols.get(headers.parent_station)
                    .map(|id| GTFSStationId::from(id.to_string())),
            )
            else {
                continue;
            };

            match location_type {
                1 => {
                    stations.push(GTFSStation::new(
                        GTFSStationId::from(id.to_string()),
                        name.to_string(),
                        lat,
                        lon,
                        vec![],
                    ));
                }
                0 => match stations
                    .iter_mut()
                    .find(|station| station.id == parent_station)
                {
                    Some(parent) => {
                        parent.stops.push(GTFSStopId::from(id.to_string()));
                    }
                    None => orphans.push((id.to_string(), parent_station)),
                },
                _ => continue,
            }
        }

        // Try to match orphans
        for (id, parent) in orphans.iter() {
            match stations.iter_mut().find(|station| &station.id == parent) {
                Some(parent) => {
                    parent.stops.push(GTFSStopId::from(id.to_string()));
                }
                None => {
                    println!("Could not find a parent with ID {id:?} for station {parent:?}")
                }
            }
        }

        Ok(stations)
    }
}

#[cfg(test)]
mod test_gtfs_parser {

    use pretty_assertions::assert_eq;

    use super::*;

    // ── error paths ─────────────────────────────────────────────────────────

    fn missing_col_name(result: Result<Vec<GTFSStation>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn test_missing_stop_id_column() {
        let content = "stop_name,stop_lat,stop_lon,location_type,parent_station\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "stop_id");
    }

    #[test]
    fn test_missing_stop_name_column() {
        let content = "stop_id,stop_lat,stop_lon,location_type,parent_station\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "stop_name");
    }

    #[test]
    fn test_missing_stop_lat_column() {
        let content = "stop_id,stop_name,stop_lon,location_type,parent_station\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "stop_lat");
    }

    #[test]
    fn test_missing_stop_lon_column() {
        let content = "stop_id,stop_name,stop_lat,location_type,parent_station\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "stop_lon");
    }

    #[test]
    fn test_missing_location_type_column() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,parent_station\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "location_type");
    }

    #[test]
    fn test_missing_parent_station_column() {
        let content = "stop_id,stop_name,stop_lat,stop_lon,location_type\n";
        let col = missing_col_name(GTFSStationParser::from(content.to_string()).stations());
        assert_eq!(col, "parent_station");
    }

    #[test]
    fn test_empty_input_reports_missing_column() {
        let result = GTFSStationParser::from(String::new()).stations();
        assert!(matches!(result, Err(GTFSParseError::MissingColumn(_))));
    }

    // ── happy path ───────────────────────────────────────────────────────────

    #[test]
    fn test_parse_ok() {
        let content = "stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station
StopArea:OCE71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,1,
StopPoint:OCETGV INOUI-71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,0,StopArea:OCE71043075".to_string();

        let parser = GTFSStationParser::from(content);

        let stations = parser.stations();
        assert_eq!(
            stations.expect("Should be Ok()"),
            vec![GTFSStation::new(
                GTFSStationId::from("StopArea:OCE71043075".to_string()),
                "FIGUERES-VILAFANT".to_string(),
                42.2645810,
                2.94302800,
                vec![GTFSStopId::from(
                    "StopPoint:OCETGV INOUI-71043075".to_string()
                )]
            )]
        )
    }

    #[test]
    fn test_parse_ok_handle_orphans() {
        let content = "stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station
StopPoint:OCETGV INOUI-71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,0,StopArea:OCE71043075
StopArea:OCE71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,1,".to_string();

        let parser = GTFSStationParser::from(content);

        let stations = parser.stations();
        assert_eq!(
            stations.expect("Should be Ok()"),
            vec![GTFSStation::new(
                GTFSStationId::from("StopArea:OCE71043075".to_string()),
                "FIGUERES-VILAFANT".to_string(),
                42.2645810,
                2.94302800,
                vec![GTFSStopId::from(
                    "StopPoint:OCETGV INOUI-71043075".to_string()
                )]
            )]
        )
    }
}
