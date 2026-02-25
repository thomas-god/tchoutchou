use derive_more::Constructor;

use crate::infra::importers::gtfs::parser::{ImportedStation, ImportedStationId, ImportedStopId};

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
    fn headers(&self) -> Option<GTFSHeaders> {
        let first_row = self.content.split("\n").next()?;
        let mut id = None;
        let mut name = None;
        let mut lat = None;
        let mut lon = None;
        let mut location_type = None;
        let mut parent_station = None;

        for (idx, w) in first_row.split(",").enumerate() {
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

        if id.is_some()
            && name.is_some()
            && lat.is_some()
            && lon.is_some()
            && location_type.is_some()
            && parent_station.is_some()
        {
            return Some(GTFSHeaders {
                id: id.unwrap(),
                name: name.unwrap(),
                lat: lat.unwrap(),
                lon: lon.unwrap(),
                location_type: location_type.unwrap(),
                parent_station: parent_station.unwrap(),
            });
        }

        None
    }

    pub fn stations(&self) -> Option<Vec<ImportedStation>> {
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
                    .map(|id| ImportedStationId::from(id.to_string())),
            )
            else {
                continue;
            };

            match location_type {
                1 => {
                    stations.push(ImportedStation::new(
                        ImportedStationId::from(id.to_string()),
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
                        parent.stops.push(ImportedStopId::from(id.to_string()));
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
                    parent.stops.push(ImportedStopId::from(id.to_string()));
                }
                None => {
                    println!("Could not find a parent with ID {id:?} for station {parent:?}")
                }
            }
        }

        Some(stations)
    }
}

#[cfg(test)]
mod test_gtfs_parser {

    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_parse_ok() {
        let content = "stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station
StopArea:OCE71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,1,
StopPoint:OCETGV INOUI-71043075,FIGUERES-VILAFANT,,42.2645810,2.94302800,,,0,StopArea:OCE71043075".to_string();

        let parser = GTFSStationParser::from(content);

        let stations = parser.stations();
        assert_eq!(
            stations.expect("Should be Some()"),
            vec![ImportedStation::new(
                ImportedStationId::from("StopArea:OCE71043075".to_string()),
                "FIGUERES-VILAFANT".to_string(),
                42.2645810,
                2.94302800,
                vec![ImportedStopId::from(
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
            stations.expect("Should be Some()"),
            vec![ImportedStation::new(
                ImportedStationId::from("StopArea:OCE71043075".to_string()),
                "FIGUERES-VILAFANT".to_string(),
                42.2645810,
                2.94302800,
                vec![ImportedStopId::from(
                    "StopPoint:OCETGV INOUI-71043075".to_string()
                )]
            )]
        )
    }
}
