use crate::infra::importers::gtfs::{GTFSCalendar, GTFSServiceId, parsers::GTFSParseError};

struct CalendarHeader {
    service_id: usize,
    monday: usize,
    tuesday: usize,
    wednesday: usize,
    thursday: usize,
    friday: usize,
    saturday: usize,
    sunday: usize,
    start_date: usize,
    end_date: usize,
}

pub struct CalendarParser {
    content: String,
}

impl From<String> for CalendarParser {
    fn from(value: String) -> Self {
        Self { content: value }
    }
}

fn parse_bool_flag(s: &str) -> Option<bool> {
    match s.trim() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

impl CalendarParser {
    fn header(&self) -> Result<CalendarHeader, GTFSParseError> {
        let first_row = self.content.split('\n').next().unwrap_or("");
        let mut service_id = None;
        let mut monday = None;
        let mut tuesday = None;
        let mut wednesday = None;
        let mut thursday = None;
        let mut friday = None;
        let mut saturday = None;
        let mut sunday = None;
        let mut start_date = None;
        let mut end_date = None;

        for (idx, col) in first_row.split(',').enumerate() {
            match col.trim() {
                "service_id" => service_id = Some(idx),
                "monday" => monday = Some(idx),
                "tuesday" => tuesday = Some(idx),
                "wednesday" => wednesday = Some(idx),
                "thursday" => thursday = Some(idx),
                "friday" => friday = Some(idx),
                "saturday" => saturday = Some(idx),
                "sunday" => sunday = Some(idx),
                "start_date" => start_date = Some(idx),
                "end_date" => end_date = Some(idx),
                _ => {}
            }
        }

        Ok(CalendarHeader {
            service_id: service_id
                .ok_or_else(|| GTFSParseError::MissingColumn("service_id".to_string()))?,
            monday: monday.ok_or_else(|| GTFSParseError::MissingColumn("monday".to_string()))?,
            tuesday: tuesday.ok_or_else(|| GTFSParseError::MissingColumn("tuesday".to_string()))?,
            wednesday: wednesday
                .ok_or_else(|| GTFSParseError::MissingColumn("wednesday".to_string()))?,
            thursday: thursday
                .ok_or_else(|| GTFSParseError::MissingColumn("thursday".to_string()))?,
            friday: friday.ok_or_else(|| GTFSParseError::MissingColumn("friday".to_string()))?,
            saturday: saturday
                .ok_or_else(|| GTFSParseError::MissingColumn("saturday".to_string()))?,
            sunday: sunday.ok_or_else(|| GTFSParseError::MissingColumn("sunday".to_string()))?,
            start_date: start_date
                .ok_or_else(|| GTFSParseError::MissingColumn("start_date".to_string()))?,
            end_date: end_date
                .ok_or_else(|| GTFSParseError::MissingColumn("end_date".to_string()))?,
        })
    }

    pub fn parse(&self) -> Result<Vec<GTFSCalendar>, GTFSParseError> {
        let header = self.header()?;
        let mut rows = self.content.split('\n');
        let _ = rows.next();

        let mut calendars = vec![];
        for row in rows {
            if row.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = row.split(',').collect();
            let (
                Some(service_id),
                Some(monday),
                Some(tuesday),
                Some(wednesday),
                Some(thursday),
                Some(friday),
                Some(saturday),
                Some(sunday),
                Some(start_date),
                Some(end_date),
            ) = (
                cols.get(header.service_id)
                    .map(|v| GTFSServiceId::from(v.trim().to_string())),
                cols.get(header.monday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.tuesday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.wednesday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.thursday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.friday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.saturday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.sunday).and_then(|v| parse_bool_flag(v)),
                cols.get(header.start_date).map(|v| v.trim().to_string()),
                cols.get(header.end_date).map(|v| v.trim().to_string()),
            )
            else {
                continue;
            };

            calendars.push(GTFSCalendar::new(
                service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
                start_date, end_date,
            ));
        }

        Ok(calendars)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn svc(id: &str) -> GTFSServiceId {
        GTFSServiceId::from(id.to_string())
    }

    fn cal(
        service_id: &str,
        mon: bool,
        tue: bool,
        wed: bool,
        thu: bool,
        fri: bool,
        sat: bool,
        sun: bool,
        start: &str,
        end: &str,
    ) -> GTFSCalendar {
        GTFSCalendar::new(
            svc(service_id),
            mon,
            tue,
            wed,
            thu,
            fri,
            sat,
            sun,
            start.to_string(),
            end.to_string(),
        )
    }

    // ── error paths ────────────────────────────────────────────────────────

    fn missing_col(result: Result<Vec<GTFSCalendar>, GTFSParseError>) -> String {
        match result.unwrap_err() {
            GTFSParseError::MissingColumn(col) => col,
            other => panic!("expected MissingColumn, got {other:?}"),
        }
    }

    #[test]
    fn missing_service_id_column() {
        let col = missing_col(
            CalendarParser::from(
                "monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    .to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "service_id");
    }

    #[test]
    fn missing_monday_column() {
        let col = missing_col(
            CalendarParser::from(
                "service_id,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    .to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "monday");
    }

    #[test]
    fn missing_start_date_column() {
        let col = missing_col(
            CalendarParser::from(
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,end_date\n"
                    .to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "start_date");
    }

    #[test]
    fn missing_end_date_column() {
        let col = missing_col(
            CalendarParser::from(
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date\n"
                    .to_string(),
            )
            .parse(),
        );
        assert_eq!(col, "end_date");
    }

    // ── round-trip parsing ─────────────────────────────────────────────────

    #[test]
    fn parses_weekday_only_service() {
        let content = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n\
                       SVC1,1,1,1,1,1,0,0,20260101,20261231\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert_eq!(
            result,
            vec![cal(
                "SVC1", true, true, true, true, true, false, false, "20260101", "20261231"
            )]
        );
    }

    #[test]
    fn parses_weekend_only_service() {
        let content = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n\
                       SVC2,0,0,0,0,0,1,1,20260301,20260630\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert_eq!(
            result,
            vec![cal(
                "SVC2", false, false, false, false, false, true, true, "20260301", "20260630"
            )]
        );
    }

    #[test]
    fn parses_multiple_rows() {
        let content = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n\
                       SVC1,1,1,1,1,1,0,0,20260101,20261231\n\
                       SVC2,0,0,0,0,0,1,1,20260101,20261231\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            cal(
                "SVC1", true, true, true, true, true, false, false, "20260101", "20261231"
            )
        );
        assert_eq!(
            result[1],
            cal(
                "SVC2", false, false, false, false, false, true, true, "20260101", "20261231"
            )
        );
    }

    #[test]
    fn empty_content_after_header_returns_empty_vec() {
        let content = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn columns_can_be_in_any_order() {
        let content = "end_date,service_id,sunday,saturday,friday,thursday,wednesday,tuesday,monday,start_date\n\
                       20261231,SVC1,0,0,1,1,1,1,1,20260101\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert_eq!(
            result,
            vec![cal(
                "SVC1", true, true, true, true, true, false, false, "20260101", "20261231"
            )]
        );
    }

    #[test]
    fn malformed_rows_are_skipped() {
        let content = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n\
                       SVC1,1,1,1,1,1,0,0,20260101,20261231\n\
                       too,short\n\
                       SVC2,0,0,0,0,0,1,1,20260101,20261231\n"
            .to_string();
        let result = CalendarParser::from(content).parse().unwrap();
        assert_eq!(result.len(), 2);
    }
}
