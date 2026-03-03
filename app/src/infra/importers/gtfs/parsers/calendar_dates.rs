use std::str::FromStr;

use crate::infra::importers::gtfs::{
    GTFSCalendarDate, GTFSExceptionType, GTFSServiceId, parsers::GTFSParseError,
};

struct CalendarDatesHeader {
    service_id: usize,
    date: usize,
    exception_type: usize,
}

pub struct CalendarDatesParser {
    content: String,
}

impl From<String> for CalendarDatesParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

impl CalendarDatesParser {
    fn header(&self) -> Result<CalendarDatesHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut service_id = None;
        let mut date = None;
        let mut exception_type = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col.trim() {
                "service_id" => service_id = Some(idx),
                "date" => date = Some(idx),
                "exception_type" => exception_type = Some(idx),
                _ => {}
            }
        }

        Ok(CalendarDatesHeader {
            service_id: service_id
                .ok_or_else(|| GTFSParseError::MissingColumn("service_id".to_string()))?,
            date: date.ok_or_else(|| GTFSParseError::MissingColumn("date".to_string()))?,
            exception_type: exception_type
                .ok_or_else(|| GTFSParseError::MissingColumn("exception_type".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSCalendarDate>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut dates = vec![];
        for row in rows {
            let cols: Vec<&str> = row.split(',').collect();
            let (Some(service_id), Some(date), Some(exception_type)) = (
                cols.get(header.service_id)
                    .map(|v| GTFSServiceId::from(v.to_string())),
                cols.get(header.date).map(|v| v.to_string()),
                cols.get(header.exception_type)
                    .and_then(|v| GTFSExceptionType::from_str(v).ok()),
            ) else {
                continue;
            };

            dates.push(GTFSCalendarDate::new(service_id, date, exception_type));
        }

        Ok(dates)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn svc(id: &str) -> GTFSServiceId {
        GTFSServiceId::from(id.to_string())
    }

    fn date(service_id: &str, date: &str, exception_type: GTFSExceptionType) -> GTFSCalendarDate {
        GTFSCalendarDate::new(svc(service_id), date.to_string(), exception_type)
    }

    // ── error paths ────────────────────────────────────────────────────────

    fn missing_col(result: Result<Vec<GTFSCalendarDate>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_service_id_column() {
        let col =
            missing_col(CalendarDatesParser::from("date,exception_type\n".to_string()).parse());
        assert_eq!(col, "service_id");
    }

    #[test]
    fn missing_date_column() {
        let col = missing_col(
            CalendarDatesParser::from("service_id,exception_type\n".to_string()).parse(),
        );
        assert_eq!(col, "date");
    }

    #[test]
    fn missing_exception_type_column() {
        let col = missing_col(CalendarDatesParser::from("service_id,date\n".to_string()).parse());
        assert_eq!(col, "exception_type");
    }

    // ── happy path ─────────────────────────────────────────────────────────

    #[test]
    fn header_only_yields_empty_vec() {
        let content = "service_id,date,exception_type\n";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn single_row_parsed_correctly() {
        let content = "service_id,date,exception_type\nSVC1,20260501,1";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(
            result,
            vec![date("SVC1", "20260501", GTFSExceptionType::ServiceAdded)]
        );
    }

    #[test]
    fn multiple_rows_for_same_service() {
        let content =
            "service_id,date,exception_type\nSVC1,20260501,1\nSVC1,20260508,1\nSVC1,20260515,2";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result.len(), 3);
        // All rows are kept, including ServiceRemoved; filtering is the importer's job.
        assert_eq!(
            result[2].exception_type(),
            GTFSExceptionType::ServiceRemoved
        );
    }

    #[test]
    fn rows_from_multiple_services_are_all_emitted() {
        let content = "service_id,date,exception_type\nSVC1,20260501,1\nSVC2,20260502,1";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].service_id(), &svc("SVC1"));
        assert_eq!(result[1].service_id(), &svc("SVC2"));
    }

    #[test]
    fn row_with_unrecognised_exception_type_is_skipped() {
        let content =
            "service_id,date,exception_type\nSVC1,20260501,BAD\nSVC1,20260501,99\nSVC1,20260508,1";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].date(), "20260508");
    }

    #[test]
    fn both_exception_types_are_parsed() {
        let content = "service_id,date,exception_type\nSVC1,20260501,1\nSVC1,20260508,2";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result[0].exception_type(), GTFSExceptionType::ServiceAdded);
        assert_eq!(
            result[1].exception_type(),
            GTFSExceptionType::ServiceRemoved
        );
    }

    #[test]
    fn truncated_row_is_skipped() {
        let content = "service_id,date,exception_type\nSVC1\nSVC2,20260501,1";
        let result = CalendarDatesParser::from(content.to_string())
            .parse()
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].service_id(), &svc("SVC2"));
    }
}
