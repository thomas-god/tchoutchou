use std::collections::HashMap;

use rusqlite::{Connection, Result, Transaction, params};

use crate::{
    app::schedulev2::{
        CityInformation, ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation,
        ImportedStationId, ImportedTripLeg, InternalStationId, InternalTripLeg,
        ScheduleDataImportResult, ScheduleDataRepository, ScheduleDataToImport,
    },
    domain::optim::{City, CityId},
};

pub struct SqliteRepository {
    conn: Connection,
}

impl SqliteRepository {
    /// Open (or create) a SQLite database at the given file path.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Create a fully in-memory SQLite database.  Useful in tests.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        // SQLite disables FK enforcement by default; turn it on for every connection.
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA optimize;
        ",
        )?;
        let repo = Self { conn };
        repo.init_schema()?;
        Ok(repo)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS t_cities (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL,
                name    TEXT NOT NULL,
                UNIQUE (country, name)

            );

            CREATE TABLE IF NOT EXISTS t_stations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                source_id   TEXT NOT NULL,
                name        TEXT NOT NULL,
                lat         REAL NOT NULL,
                lon         REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS t_station_to_city (
                station_id  INTEGER REFERENCES t_stations(id) ON DELETE CASCADE,
                city_id     INTEGER REFERENCES t_cities(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS t_schedules (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                source_id   TEXT NOT NULL
            );

            -- One row per (schedule, date); indexed for fast per-date trip retrieval.
            CREATE TABLE IF NOT EXISTS t_schedule_dates (
                schedule_id INTEGER NOT NULL REFERENCES t_schedules(id) ON DELETE CASCADE,
                source      TEXT NOT NULL,
                date        TEXT NOT NULL,
                PRIMARY KEY (schedule_id, date)
            );
            CREATE INDEX IF NOT EXISTS idx_schedule_dates_date
                ON t_schedule_dates (date, schedule_id, source);

            CREATE TABLE IF NOT EXISTS t_trips (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT    NOT NULL,
                route       TEXT    NOT NULL,
                origin      INTEGER NOT NULL REFERENCES t_stations(id) ON DELETE CASCADE,
                destination INTEGER NOT NULL REFERENCES t_stations(id) ON DELETE CASCADE,
                departure   INTEGER NOT NULL,
                arrival     INTEGER NOT NULL,
                UNIQUE (source, origin, destination, departure, arrival) ON CONFLICT IGNORE
            );
            CREATE INDEX IF NOT EXISTS idx_trips_source_route ON t_trips (source, route);
            CREATE INDEX IF NOT EXISTS idx_trips_route_source ON t_trips (route, source);

            CREATE TABLE IF NOT EXISTS t_route_schedules (
                source      TEXT NOT NULL,
                route_id    TEXT NOT NULL,
                schedule_id TEXT NOT NULL,
                PRIMARY KEY (source, route_id, schedule_id)
            );
            CREATE INDEX IF NOT EXISTS idx_route_schedules
                ON t_route_schedules (schedule_id, source, route_id);

            PRAGMA optimize;
            ",
        )
    }
}

impl SqliteRepository {
    /// Delete all timetable rows for `source` in a single batch.
    fn truncate_timetable(tx: &Transaction, source: &str) {
        tx.prepare_cached("DELETE FROM t_stations WHERE source = ?1;")
            .expect("delete_from_route_schedules: prepare failed")
            .execute(params![source])
            .expect("delete_from_t_stations: execute failed");

        tx.prepare_cached("DELETE FROM t_route_schedules WHERE source = ?1;")
            .expect("delete_from_route_schedules: prepare failed")
            .execute(params![source])
            .expect("delete_from_route_schedules: execute failed");

        tx.prepare_cached("DELETE FROM t_schedule_dates WHERE source = ?1;")
            .expect("delete_schedule_dates: prepare failed")
            .execute(params![source])
            .expect("delete_schedule_dates: execute failed");

        tx.prepare_cached("DELETE FROM t_schedules WHERE source = ?1;")
            .expect("delete_schedules: prepare failed")
            .execute(params![source])
            .expect("delete_schedules: execute failed");

        tx.prepare_cached("DELETE FROM t_trips WHERE source = ?1;")
            .expect("delete_from_trips: prepare failed")
            .execute(params![source])
            .expect("delete_from_trips: execute failed");
    }

    fn insert_stations(
        tx: &Transaction,
        stations: &[ImportedStation],
        source: &str,
    ) -> HashMap<ImportedStationId, InternalStationId> {
        let mut mapping = HashMap::new();
        let mut stmt = tx
            .prepare_cached(
                "INSERT OR REPLACE INTO t_stations (source, source_id, name, lat, lon)
                 VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id;",
            )
            .expect("insert_stations: prepare failed");
        for s in stations {
            let Ok(id) = stmt.query_row(
                params![source, s.id().as_str(), s.name(), s.lat(), s.lon()],
                |row| row.get::<_, i64>(0),
            ) else {
                continue;
            };
            mapping.insert(s.id().clone(), InternalStationId::from(id));
        }
        mapping
    }

    /// Insert schedules (tables were just truncated, so no conflict is expected).
    fn insert_schedules(tx: &Transaction, schedules: &[ImportedSchedule], source: &str) {
        let mut schedule_stmt = tx
            .prepare_cached(
                "INSERT INTO t_schedules (source, source_id) VALUES (?1, ?2) RETURNING id;",
            )
            .expect("insert_schedules: prepare schedules failed");
        let mut date_stmt = tx
            .prepare_cached(
                "INSERT INTO t_schedule_dates (schedule_id, source, date) VALUES (?1, ?2, ?3)",
            )
            .expect("insert_schedules: prepare schedule_dates failed");
        for s in schedules {
            let Ok(id) = schedule_stmt
                .query_row(params![source, s.id().as_str()], |row| row.get::<_, i64>(0))
            else {
                continue;
            };
            for date in s.dates() {
                date_stmt
                    .execute(params![id, source, date])
                    .expect("insert_schedules: execute schedule_dates failed");
            }
        }
    }

    /// Insert trip legs (table was just truncated, so no conflict is expected).
    fn insert_trips(
        tx: &Transaction,
        trips: &[ImportedTripLeg],
        source: &str,
        station_mapping: &HashMap<ImportedStationId, InternalStationId>,
    ) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_trips (route, source, origin, destination, departure, arrival)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .expect("insert_trips: prepare failed");
        for t in trips {
            let (Some(origin), Some(destination)) = (
                station_mapping.get(t.origin()),
                station_mapping.get(t.destination()),
            ) else {
                continue;
            };
            stmt.execute(params![
                t.route().as_str(),
                source,
                origin.value(),
                destination.value(),
                t.departure() as i64,
                t.arrival() as i64,
            ])
            .expect("insert_trips: execute failed");
        }
    }

    /// Insert route–schedule mappings (table was just truncated).
    fn insert_route_schedules(
        tx: &Transaction,
        mapping: &HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
        source: &str,
    ) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_route_schedules (source, route_id, schedule_id)
                 VALUES (?1, ?2, ?3)",
            )
            .expect("insert_route_schedules: prepare failed");
        for (route, schedules) in mapping {
            for schedule in schedules {
                stmt.execute(params![source, route.as_str(), schedule.as_str()])
                    .expect("insert_route_schedules: execute failed");
            }
        }
    }

    fn insert_station_to_city(
        tx: &Transaction,
        station_to_city: &HashMap<ImportedStationId, CityInformation>,
        station_internal_mapping: &HashMap<ImportedStationId, InternalStationId>,
    ) {
        let mut insert_city = tx
            .prepare_cached(
                "INSERT INTO t_cities (country, name)
                VALUES (?1, ?2)
                ON CONFLICT (country, name) DO UPDATE SET country = excluded.country
                RETURNING id;",
            )
            .expect("insert_cities: prepare failed");
        let mut insert_station_to_city = tx
            .prepare_cached("INSERT INTO t_station_to_city (station_id, city_id) VALUES (?1, ?2);")
            .expect("insert_station_to_city: prepare failed");

        for (station, city) in station_to_city.iter() {
            let Some(station_id) = station_internal_mapping.get(station) else {
                continue;
            };
            let Ok(city_id) = insert_city.query_row(params![city.name(), city.country()], |row| {
                row.get::<_, i64>(0).map(CityId::from)
            }) else {
                continue;
            };

            let _ = insert_station_to_city.execute(params![station_id.value(), city_id.as_i64()]);
        }
    }
}

impl ScheduleDataRepository for SqliteRepository {
    fn import_timetable(&mut self, data: ScheduleDataToImport) -> ScheduleDataImportResult {
        let tx = self
            .conn
            .transaction()
            .expect("import_timetable: begin transaction failed");

        Self::truncate_timetable(&tx, data.source());
        let station_mapping = Self::insert_stations(&tx, data.stations(), data.source());
        Self::insert_station_to_city(&tx, data.station_to_city(), &station_mapping);
        Self::insert_schedules(&tx, data.schedules(), data.source());
        Self::insert_trips(&tx, data.trip_legs(), data.source(), &station_mapping);
        Self::insert_route_schedules(&tx, data.schedules_by_route(), data.source());

        tx.commit().expect("import_timetable: commit failed");
        let _ = self.conn.execute_batch("PRAGMA optimize;");
        tracing::info!(
            source = %data.source(),
            "import_timetable: {} stations, {} schedules, {} trips",
            data.stations().len(),
            data.schedules().len(),
            data.trip_legs().len(),
        );

        ScheduleDataImportResult {}
    }

    fn legs_for_date(&self, date: &str) -> Vec<InternalTripLeg> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "WITH active_routes AS (
                     SELECT DISTINCT rs.source, rs.route_id
                     FROM t_schedule_dates sd
                     JOIN t_route_schedules rs ON rs.schedule_id = sd.schedule_id
                     WHERE sd.date = ?1
                 )
                 SELECT t.origin, t.destination, t.departure, t.arrival
                 FROM t_trips t
                 JOIN active_routes ar ON ar.route_id = t.route AND ar.source = t.source",
            )
            .expect("trips_for_date: prepare failed");

        stmt.query_map(params![date], |row| {
            let origin: i64 = row.get(0)?;
            let destination: i64 = row.get(1)?;
            let departure: i64 = row.get(2)?;
            let arrival: i64 = row.get(3)?;
            Ok(InternalTripLeg::new(
                InternalStationId::from(origin),
                InternalStationId::from(destination),
                departure as usize,
                arrival as usize,
            ))
        })
        .expect("trips_for_date: query failed")
        .map(|r| r.expect("trips_for_date: row mapping failed"))
        .collect()
    }

    fn search_cities_by_name(&self, query: &str, limit: usize) -> Vec<City> {
        let pattern = format!("%{}%", query);
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, country, name, FROM t_cities
                 WHERE LOWER(name) LIKE LOWER(?1)
                 ORDER BY name
                 LIMIT ?2",
            )
            .expect("search_cities_by_name: prepare failed");

        stmt.query_map(params![pattern, limit as i64], |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            Ok(City::new(CityId::from(id), name, country))
        })
        .expect("search_cities_by_name: query failed")
        .map(|r| r.expect("search_cities_by_name: row mapping failed"))
        .collect()
    }

    fn stations_to_city(&self) -> HashMap<InternalStationId, CityId> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT station_id, city_id FROM t_station_to_city;")
            .expect("stations_to_city: prepare failed");

        let rows = stmt
            .query_map(params![], |row| {
                dbg!(&row);
                let station: i64 = row.get(0)?;
                let city: i64 = row.get(1)?;
                Ok((InternalStationId::from(station), CityId::from(city)))
            })
            .expect("stations_to_city: row mapping failed")
            .filter_map(|row| row.ok());
        HashMap::from_iter(rows)
    }
}

#[cfg(test)]
impl SqliteRepository {
    fn all_stations(&self) -> Vec<ImportedStation> {
        let mut stmt = self
            .conn
            .prepare("SELECT source_id, name, lat, lon FROM t_stations")
            .expect("all_stations: prepare failed");

        stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let lat: f64 = row.get(2)?;
            let lon: f64 = row.get(3)?;
            Ok(ImportedStation::new(
                ImportedStationId::from(id),
                name,
                lat,
                lon,
            ))
        })
        .expect("all_stations: query failed")
        .map(|r| r.expect("all_stations: row mapping failed"))
        .collect()
    }

    fn all_trip_legs(&self) -> Vec<InternalTripLeg> {
        let mut stmt = self
            .conn
            .prepare("SELECT origin, destination, departure, arrival FROM t_trips")
            .expect("all_trip_legs: prepare failed");

        stmt.query_map([], |row| {
            let origin: i64 = row.get(0)?;
            let destination: i64 = row.get(1)?;
            let departure: i64 = row.get(2)?;
            let arrival: i64 = row.get(3)?;
            Ok(InternalTripLeg::new(
                InternalStationId::from(origin),
                InternalStationId::from(destination),
                departure as usize,
                arrival as usize,
            ))
        })
        .expect("all_trip_legs: query failed")
        .map(|r| r.expect("all_trip_legs: row mapping failed"))
        .collect()
    }
}

#[cfg(test)]
mod test_sqlite_v2 {
    use crate::app::schedulev2::{CityInformation, TrainDataToImport};

    use super::*;

    // ---- helpers ----

    fn make_repo() -> SqliteRepository {
        SqliteRepository::open_in_memory().expect("in-memory DB failed")
    }

    fn station(id: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            1.0,
            2.0,
        )
    }
    fn schedule(id: &str, dates: &[&str]) -> ImportedSchedule {
        ImportedSchedule::new(
            ImportedScheduleId::from(id.to_owned()),
            dates.iter().map(|d| d.to_string()).collect(),
        )
    }

    fn trip(route: &str, from: &str, to: &str, dep: usize, arr: usize) -> ImportedTripLeg {
        ImportedTripLeg::new(
            ImportedRouteId::from(route.to_owned()),
            ImportedStationId::from(from.to_owned()),
            ImportedStationId::from(to.to_owned()),
            dep,
            arr,
        )
    }
    fn data_to_import(
        stations: Vec<ImportedStation>,
        schedules: Vec<ImportedSchedule>,
        trips: Vec<ImportedTripLeg>,
        source: &str,
        mapping: HashMap<ImportedStationId, CityInformation>,
    ) -> ScheduleDataToImport {
        let mut sbr = std::collections::HashMap::new();
        for s in &schedules {
            sbr.entry(ImportedRouteId::from("R1".to_owned()))
                .or_insert_with(Vec::new)
                .push(s.id().clone());
        }
        let train_data = TrainDataToImport::new(stations, trips, schedules, sbr, source.to_owned());

        ScheduleDataToImport::new(train_data, mapping)
    }

    #[test]
    fn test_ingest_ok() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new("name".to_string(), "country".to_string()),
            )]),
        ));
    }

    #[test]
    fn test_ingest_idempotent() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new("name".to_string(), "country".to_string()),
            )]),
        );
        repo.import_timetable(data.clone());
        assert_eq!(repo.all_stations().len(), 2);
        assert_eq!(repo.all_trip_legs().len(), 1);

        repo.import_timetable(data.clone());
        assert_eq!(repo.all_stations().len(), 2);
        assert_eq!(repo.all_trip_legs().len(), 1);
    }

    #[test]
    fn test_ingest_empty_data_clean_tables() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new("name".to_string(), "country".to_string()),
            )]),
        );
        repo.import_timetable(data.clone());
        assert_eq!(repo.all_stations().len(), 2);
        assert_eq!(repo.all_trip_legs().len(), 1);

        let empty_data = ScheduleDataToImport::new(
            TrainDataToImport::new(vec![], vec![], vec![], HashMap::new(), "source".to_string()),
            HashMap::new(),
        );
        repo.import_timetable(empty_data);
        assert_eq!(repo.all_stations().len(), 0);
        assert_eq!(repo.all_trip_legs().len(), 0);
    }

    #[test]
    fn test_station_to_city_mapping() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new("name".to_string(), "country".to_string()),
            )]),
        );

        repo.import_timetable(data);

        let mapping = repo.stations_to_city();
        assert_eq!(mapping.len(), 1)
    }

    #[test]
    fn test_station_to_city_mapping_multiple_stations_same_city() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new("name".to_string(), "country".to_string()),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new("name".to_string(), "country".to_string()),
                ),
            ]),
        );

        repo.import_timetable(data);

        let mapping = repo.stations_to_city();
        dbg!(&mapping);
        assert_eq!(mapping.len(), 2)
    }
}
