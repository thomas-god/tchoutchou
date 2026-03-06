use std::str::FromStr;

use crate::infra::importers::gtfs::{
    GTFSRoute, GTFSRouteId, GTFSRouteType, parsers::GTFSParseError,
};

struct RoutesHeader {
    route_id: usize,
    route_type: usize,
}

pub struct RoutesParser {
    content: String,
}

impl From<String> for RoutesParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl RoutesParser {
    fn header(&self) -> Result<RoutesHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut route_id = None;
        let mut route_type = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col.trim() {
                "route_id" => route_id = Some(idx),
                "route_type" => route_type = Some(idx),
                _ => {}
            }
        }

        Ok(RoutesHeader {
            route_id: route_id
                .ok_or_else(|| GTFSParseError::MissingColumn("route_id".to_string()))?,
            route_type: route_type
                .ok_or_else(|| GTFSParseError::MissingColumn("route_type".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSRoute>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut routes = vec![];
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(route_id), Some(route_type)) = (
                cols.get(header.route_id)
                    .map(|v| GTFSRouteId::from(v.to_string())),
                cols.get(header.route_type)
                    .and_then(|v| GTFSRouteType::from_str(v).ok()),
            ) else {
                continue;
            };

            routes.push(GTFSRoute::new(route_id, route_type));
        }

        Ok(routes)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn route_id(id: &str) -> GTFSRouteId {
        GTFSRouteId::from(id.to_string())
    }

    #[test]
    fn test_parse_routes() {
        let content = "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color\n\
                       ROUTE_RAIL,1187,TGV,Paris - Lyon,,2,,0749FF,FFFFFF\n\
                       ROUTE_BUS,1187,B1,City Bus,,3,,006600,FFFFFF\n\
                       ROUTE_FERRY,1187,F1,River Ferry,,4,,0000FF,FFFFFF"
            .to_string();

        let parser = RoutesParser::from(content);
        let routes = parser.parse().expect("parse should succeed");

        assert_eq!(
            routes,
            vec![
                GTFSRoute::new(route_id("ROUTE_RAIL"), GTFSRouteType::Rail),
                GTFSRoute::new(route_id("ROUTE_BUS"), GTFSRouteType::Bus),
                GTFSRoute::new(route_id("ROUTE_FERRY"), GTFSRouteType::Ferry),
            ]
        );
    }

    #[test]
    fn test_parse_skips_rows_with_unknown_route_type() {
        let content = "route_id,route_type\n\
                       KNOWN,2\n\
                       UNKNOWN,99\n\
                       ALSO_KNOWN,3"
            .to_string();

        let routes = RoutesParser::from(content).parse().unwrap();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].route_id(), &route_id("KNOWN"));
        assert_eq!(routes[1].route_id(), &route_id("ALSO_KNOWN"));
    }

    #[test]
    fn test_parse_missing_route_id_column() {
        let content = "route_type\n2".to_string();
        let err = RoutesParser::from(content).parse().unwrap_err();
        assert!(matches!(err, GTFSParseError::MissingColumn(_)));
    }

    #[test]
    fn test_parse_missing_route_type_column() {
        let content = "route_id\nROUTE1".to_string();
        let err = RoutesParser::from(content).parse().unwrap_err();
        assert!(matches!(err, GTFSParseError::MissingColumn(_)));
    }

    #[test]
    fn test_parse_all_route_types() {
        let content = "route_id,route_type\n\
                       R0,0\nR1,1\nR2,2\nR3,3\nR4,4\nR5,5\nR6,6\nR7,7\nR11,11\nR12,12"
            .to_string();

        let routes = RoutesParser::from(content).parse().unwrap();
        assert_eq!(routes.len(), 10);
        assert_eq!(routes[0].route_type(), GTFSRouteType::Tram);
        assert_eq!(routes[1].route_type(), GTFSRouteType::Subway);
        assert_eq!(routes[2].route_type(), GTFSRouteType::Rail);
        assert_eq!(routes[3].route_type(), GTFSRouteType::Bus);
        assert_eq!(routes[4].route_type(), GTFSRouteType::Ferry);
        assert_eq!(routes[5].route_type(), GTFSRouteType::CableTram);
        assert_eq!(routes[6].route_type(), GTFSRouteType::AerialLift);
        assert_eq!(routes[7].route_type(), GTFSRouteType::Funicular);
        assert_eq!(routes[8].route_type(), GTFSRouteType::Trolleybus);
        assert_eq!(routes[9].route_type(), GTFSRouteType::Monorail);
    }
}
