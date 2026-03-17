use std::collections::HashMap;

use derive_more::From;
use rusqlite::{
    Connection, Result, ToSql, Transaction, params,
    types::{ToSqlOutput, ValueRef},
};

use crate::{
    app::{
        ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
        ImportedTripLeg,
        schedule::{
            AddLabelToCityError, CityImportKey, CityInformation, CityWithExtraInformation,
            InternalStationId, InternalTripLeg, LabelCreationError, ScheduleDataImportResult,
            ScheduleDataRepository, ScheduleDataToImport,
        },
    },
    domain::{City, CityCountry, CityId, CityLabelId, CityLabelName, CityLabels, CityName},
};

pub struct SqliteRepository {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
struct InternalScheduleId(i64);

impl InternalScheduleId {
    fn value(&self) -> &i64 {
        &self.0
    }
}

impl ToSql for CityName {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(
            (self.as_ref() as &str).as_bytes(),
        )))
    }
}

impl ToSql for CityCountry {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(
            (self.as_ref() as &str).as_bytes(),
        )))
    }
}

impl ToSql for CityImportKey {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(
            (self.as_ref() as &str).as_bytes(),
        )))
    }
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
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                country     TEXT NOT NULL,
                name        TEXT NOT NULL,
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                wikidata    TEXT,
                wikipedia   TEXT,
                import_key  TEXT NOT NULL,
                parent      INTEGER REFERENCES t_cities(id),
                UNIQUE (import_key)
            );

            CREATE TABLE IF NOT EXISTS t_city_labels (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS t_city_to_label (
                city_id     INTEGER REFERENCES t_cities(id) ON DELETE CASCADE,
                label_id    INTEGER REFERENCES t_city_labels(id) ON DELETE CASCADE,
                PRIMARY KEY (city_id, label_id)

            );

            CREATE VIEW IF NOT EXISTS v_cities_with_labels AS
                WITH cities_with_label AS (
                    SELECT
                        t_cities.id,
                        country,
                        t_cities.name,
                        lat,
                        lon,
                        wikidata,
                        wikipedia,
                        CASE WHEN
                            t_city_labels.id IS NULL THEN NULL
                            ELSE json_object('id', t_city_labels.id, 'name', t_city_labels.name)
                        END AS label
                    FROM t_cities
                    LEFT JOIN t_city_to_label ON t_cities.id = t_city_to_label.city_id
                    LEFT JOIN t_city_labels ON t_city_labels.id = t_city_to_label.label_id
                )
                SELECT
                    id,
                    country,
                    name,
                    lat,
                    lon,
                    wikidata,
                    wikipedia,
                    json_group_array(label) FILTER (WHERE label IS NOT NULL)
                FROM cities_with_label
                GROUP BY id, country, name, lat, lon, wikidata, wikipedia;


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
                schedule_id INTEGER NOT NULL REFERENCES t_schedules(id) ON DELETE CASCADE,
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
    fn insert_schedules(
        tx: &Transaction,
        schedules: &[ImportedSchedule],
        source: &str,
    ) -> HashMap<ImportedScheduleId, InternalScheduleId> {
        let mut map = HashMap::new();
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
            map.insert(s.id().clone(), InternalScheduleId::from(id));
            for date in s.dates() {
                date_stmt
                    .execute(params![id, source, date])
                    .expect("insert_schedules: execute schedule_dates failed");
            }
        }

        map
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
        schedule_mapping: &HashMap<ImportedScheduleId, InternalScheduleId>,
    ) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO t_route_schedules (source, route_id, schedule_id)
                 VALUES (?1, ?2, ?3)",
            )
            .expect("insert_route_schedules: prepare failed");
        for (route, schedules) in mapping {
            for schedule in schedules {
                let Some(sid) = schedule_mapping.get(schedule) else {
                    continue;
                };
                stmt.execute(params![source, route.as_str(), sid.value()])
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
                "INSERT INTO t_cities (import_key, country, name, lat, lon, wikidata, wikipedia)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT (import_key) DO UPDATE SET
                    lat = excluded.lat,
                    lon = excluded.lon,
                    wikidata = excluded.wikidata,
                    wikipedia = excluded.wikipedia
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
            let Ok(city_id) = insert_city.query_row(
                params![
                    city.import_key(),
                    city.country(),
                    city.name(),
                    city.lat(),
                    city.lon(),
                    city.wikidata(),
                    city.wikipedia()
                ],
                |row| row.get::<_, i64>(0).map(CityId::from),
            ) else {
                continue;
            };

            let _ = insert_station_to_city.execute(params![station_id.value(), *city_id]);
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
        let schedule_mapping = Self::insert_schedules(&tx, data.schedules(), data.source());
        Self::insert_trips(&tx, data.trip_legs(), data.source(), &station_mapping);
        Self::insert_route_schedules(
            &tx,
            data.schedules_by_route(),
            data.source(),
            &schedule_mapping,
        );

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
                "SELECT id, country, name, lat, lon, parent FROM t_cities
                 WHERE LOWER(name) LIKE LOWER(?1)
                 ORDER BY name
                 LIMIT ?2",
            )
            .expect("search_cities_by_name: prepare failed");

        stmt.query_map(params![pattern, limit as i64], |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            let lat: f64 = row.get(3)?;
            let lon: f64 = row.get(4)?;
            let parent: Option<i64> = row.get(5)?;
            Ok(City::new(
                CityId::from(id),
                name.into(),
                country.into(),
                lat,
                lon,
                parent.map(CityId::from),
                CityLabels::empty(),
            ))
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
                let station: i64 = row.get(0)?;
                let city: i64 = row.get(1)?;
                Ok((InternalStationId::from(station), CityId::from(city)))
            })
            .expect("stations_to_city: row mapping failed")
            .filter_map(|row| row.ok());
        HashMap::from_iter(rows)
    }

    fn cities_by_ids(&self, ids: &[CityId]) -> Vec<City> {
        if ids.is_empty() {
            return vec![];
        }

        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "SELECT id, country, name, lat, lon, parent FROM t_cities WHERE id IN ({}) ORDER BY id",
            placeholders
        );

        let mut stmt = self
            .conn
            .prepare(&query)
            .expect("cities_by_ids: prepare failed");
        let params: Vec<i64> = ids.iter().map(|id| **id).collect();

        stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            let lat: f64 = row.get(3)?;
            let lon: f64 = row.get(4)?;
            let parent: Option<i64> = row.get(5)?;
            Ok(City::new(
                CityId::from(id),
                name.into(),
                country.into(),
                lat,
                lon,
                parent.map(CityId::from),
                CityLabels::empty(),
            ))
        })
        .expect("cities_by_ids: query failed")
        .map(|r| r.expect("cities_by_ids: row mapping failed"))
        .collect()
    }

    fn all_cities(&self) -> Vec<City> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, country, name, lat, lon, parent FROM t_cities ORDER BY id")
            .expect("all_cities: prepare failed");

        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            let lat: f64 = row.get(3)?;
            let lon: f64 = row.get(4)?;
            let parent: Option<i64> = row.get(5)?;
            Ok(City::new(
                CityId::from(id),
                name.into(),
                country.into(),
                lat,
                lon,
                parent.map(CityId::from),
                CityLabels::empty(),
            ))
        })
        .expect("all_cities: query failed")
        .map(|r| r.expect("all_cities: row mapping failed"))
        .collect()
    }

    fn all_cities_with_extra_information(&self) -> Vec<CityWithExtraInformation> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, country, name, lat, lon, parent, wikidata, wikipedia \
                 FROM t_cities ORDER BY id",
            )
            .expect("all_cities_with_extra_information: prepare failed");

        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            let lat: f64 = row.get(3)?;
            let lon: f64 = row.get(4)?;
            let parent: Option<i64> = row.get(5)?;
            let wikidata: Option<String> = row.get(6)?;
            let wikipedia: Option<String> = row.get(7)?;
            Ok(CityWithExtraInformation {
                city: City::new(
                    CityId::from(id),
                    name.into(),
                    country.into(),
                    lat,
                    lon,
                    parent.map(CityId::from),
                    CityLabels::empty(),
                ),
                wikidata,
                wikipedia,
            })
        })
        .expect("all_cities_with_extra_information: query failed")
        .map(|r| r.expect("all_cities_with_extra_information: row mapping failed"))
        .collect()
    }

    fn create_label(&mut self, name: CityLabelName) -> Result<CityLabelId, LabelCreationError> {
        self.conn
            .prepare_cached("INSERT INTO t_city_labels (name) VALUES (?1) RETURNING id;")
            .expect("create_label: prepare failed")
            .query_row(params![AsRef::<str>::as_ref(&name)], |row| {
                row.get::<_, i64>(0)
            })
            .map(CityLabelId::from)
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(err, _)
                    if err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    LabelCreationError::LabelNameAlreadyExists
                }
                _ => panic!("create_label: unexpected error: {e}"),
            })
    }

    fn add_label_to_city(
        &mut self,
        city: &CityId,
        label: &CityLabelId,
    ) -> Result<(), AddLabelToCityError> {
        let city_exists = self
            .conn
            .prepare_cached("SELECT 1 FROM t_cities WHERE id = ?1")
            .expect("add_label_to_city: prepare city check failed")
            .exists(params![**city])
            .expect("add_label_to_city: city check failed");
        if !city_exists {
            return Err(AddLabelToCityError::CityNotFound);
        }

        let label_exists = self
            .conn
            .prepare_cached("SELECT 1 FROM t_city_labels WHERE id = ?1")
            .expect("add_label_to_city: prepare label check failed")
            .exists(params![**label])
            .expect("add_label_to_city: label check failed");
        if !label_exists {
            return Err(AddLabelToCityError::LabelNotFound);
        }

        self.conn
            .prepare_cached(
                "INSERT OR IGNORE INTO t_city_to_label (city_id, label_id) VALUES (?1, ?2);",
            )
            .expect("add_label_to_city: prepare failed")
            .execute(params![**city, **label])
            .expect("add_label_to_city: execute failed");

        Ok(())
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

    fn all_cities(&self) -> Vec<City> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, country, name, lat, lon, parent FROM t_cities ORDER BY id")
            .expect("all_cities: prepare failed");

        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let country: String = row.get(1)?;
            let name: String = row.get(2)?;
            let lat: f64 = row.get(3)?;
            let lon: f64 = row.get(4)?;
            let parent: Option<i64> = row.get(5)?;
            Ok(City::new(
                CityId::from(id),
                name.into(),
                country.into(),
                lat,
                lon,
                parent.map(CityId::from),
                CityLabels::empty(),
            ))
        })
        .expect("all_cities: query failed")
        .map(|r| r.expect("all_cities: row mapping failed"))
        .collect()
    }

    /// Test helper to verify wikidata/wikipedia are stored correctly
    fn get_city_metadata(&self, city_id: i64) -> Option<(Option<String>, Option<String>)> {
        self.conn
            .query_row(
                "SELECT wikidata, wikipedia FROM t_cities WHERE id = ?1",
                params![city_id],
                |row| {
                    let wikidata: Option<String> = row.get(0)?;
                    let wikipedia: Option<String> = row.get(1)?;
                    Ok((wikidata, wikipedia))
                },
            )
            .ok()
    }
}

#[cfg(test)]
mod test_sqlite {
    use crate::app::{TrainDataToImport, schedule::CityInformation};

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
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "city-A".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "city-B".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
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
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "city-A".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "city-B".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
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
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "city-A".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "city-B".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
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
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "city-A".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key-1".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "city-B".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key-2".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        let mapping = repo.stations_to_city();
        assert_eq!(mapping.len(), 2);
        assert_ne!(
            mapping.get(&InternalStationId::from(1)),
            mapping.get(&InternalStationId::from(2)),
        );
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
                    CityInformation::new(
                        "same-city".into(),
                        "same-country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "same-city".into(),
                        "same-country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        let mapping = repo.stations_to_city();
        assert_eq!(mapping.len(), 2);
        assert_eq!(
            mapping.get(&InternalStationId::from(1)),
            mapping.get(&InternalStationId::from(2)),
        );
    }

    #[test]
    fn test_trip_legs() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "city-A".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "city-B".into(),
                        "country".into(),
                        0.0,
                        0.0,
                        "key".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        let legs = repo.legs_for_date("20260101");
        assert_eq!(
            legs,
            vec![InternalTripLeg::new(
                InternalStationId::from(1),
                InternalStationId::from(2),
                100,
                200
            )]
        );
    }

    #[test]
    fn test_city_name_and_country_correctly_stored() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key".into(),
                    Some("wikidata".to_string()),
                    Some("wikipedia".to_string()),
                ),
            )]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        assert_eq!(cities.len(), 1);
        let city = &cities[0];
        assert_eq!(*city.name(), "Paris".into());
        assert_eq!(*city.country(), "France".into());
        assert_eq!(city.lat(), 48.8566);
        assert_eq!(city.lon(), 2.3522);
    }

    #[test]
    fn test_cities_by_ids_returns_empty_for_empty_input() {
        let repo = make_repo();
        let cities = repo.cities_by_ids(&[]);
        assert_eq!(cities.len(), 0);
    }

    #[test]
    fn test_cities_by_ids_returns_requested_cities() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B"), station("C")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Paris".into(),
                        "France".into(),
                        48.8566,
                        2.3522,
                        "key-1".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "London".into(),
                        "UK".into(),
                        51.5074,
                        -0.1278,
                        "key-2".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("C".to_string()),
                    CityInformation::new(
                        "Berlin".into(),
                        "Germany".into(),
                        52.5200,
                        13.4050,
                        "key-3".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        // Get all cities first to know their IDs
        let all_cities = repo.all_cities();
        assert_eq!(all_cities.len(), 3);

        // Request specific cities by ID
        let requested_ids: Vec<CityId> = all_cities.iter().take(2).map(|c| *c.id()).collect();
        let cities = repo.cities_by_ids(&requested_ids);

        assert_eq!(cities.len(), 2);
        assert!(cities.iter().any(|c| *c.name() == "Paris".into()
            || *c.name() == "London".into()
            || *c.name() == "Berlin".into()));
    }

    #[test]
    fn test_cities_by_ids_handles_nonexistent_ids() {
        let repo = make_repo();

        // Request cities that don't exist
        let cities = repo.cities_by_ids(&[CityId::from(999), CityId::from(1000)]);

        assert_eq!(cities.len(), 0);
    }

    #[test]
    fn test_cities_by_ids_returns_in_order() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Paris".into(),
                        "France".into(),
                        48.8566,
                        2.3522,
                        "key-1".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "London".into(),
                        "UK".into(),
                        51.5074,
                        -0.1278,
                        "key-2".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        let all_cities = repo.all_cities();
        let city_ids: Vec<CityId> = all_cities.iter().map(|c| *c.id()).collect();

        let cities = repo.cities_by_ids(&city_ids);

        // Verify cities are returned in ID order
        assert_eq!(cities.len(), 2);
        for i in 1..cities.len() {
            assert!(cities[i - 1].id() <= cities[i].id());
        }
    }

    #[test]
    fn test_same_city_name_and_country_different_coordinates_creates_two_rows() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Paris".into(),
                        "France".into(),
                        48.8566,
                        2.3522,
                        "key-1".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "Paris".into(),
                        "France".into(),
                        48.9000,
                        2.4000,
                        "key-2".into(),
                        Some("wikidata".to_string()),
                        Some("wikipedia".to_string()),
                    ),
                ),
            ]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        assert_eq!(
            cities.len(),
            2,
            "Should create two separate city rows for same name/country with different coordinates"
        );

        // Verify both cities have the same name and country but different coordinates
        assert_eq!(*cities[0].name(), "Paris".into());
        assert_eq!(*cities[0].country(), "France".into());
        assert_eq!(*cities[1].name(), "Paris".into());
        assert_eq!(*cities[1].country(), "France".into());

        // Verify coordinates are different
        assert_ne!(
            (cities[0].lat(), cities[0].lon()),
            (cities[1].lat(), cities[1].lon()),
            "The two Paris cities should have different coordinates"
        );

        // Verify they have different IDs
        assert_ne!(cities[0].id(), cities[1].id());

        // Verify both stations map to different cities
        let mapping = repo.stations_to_city();
        assert_eq!(mapping.len(), 2);
        assert_ne!(
            mapping.get(&InternalStationId::from(1)),
            mapping.get(&InternalStationId::from(2)),
            "Different coordinates should result in different city IDs"
        );
    }

    #[test]
    fn test_reimport_same_import_key_updates_city_metadata_but_name() {
        // A second import with the same import_key should update the
        // existing city row metadata rather than create a duplicate.
        let mut repo = make_repo();

        let first = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Old Name".into(),
                    "France".into(),
                    1.0,
                    2.0,
                    "key-paris".into(),
                    Some("wikidata".to_string()),
                    Some("wikipedia".to_string()),
                ),
            )]),
        );
        repo.import_timetable(first);

        let second = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "New name".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("wikidata".to_string()),
                    Some("wikipedia".to_string()),
                ),
            )]),
        );
        repo.import_timetable(second);

        let cities = repo.all_cities();
        assert_eq!(
            cities.len(),
            1,
            "Reimport with same import_key must not create a duplicate city row"
        );
        assert_eq!(
            *cities[0].name(),
            "Old Name".into(),
            "City name should not be updated on reimport"
        );
        assert_eq!(cities[0].lat(), 48.8566);
    }

    #[test]
    fn test_cross_source_same_import_key_resolves_to_same_city() {
        // Two different sources referencing the same import_key should both
        // end up pointing at the same city row.
        let mut repo = make_repo();

        let source_a = data_to_import(
            vec![station("X")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source-a",
            HashMap::from([(
                ImportedStationId::from("X".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("wikidata".to_string()),
                    Some("wikipedia".to_string()),
                ),
            )]),
        );
        repo.import_timetable(source_a);

        let source_b = data_to_import(
            vec![station("Y")],
            vec![schedule("S2", &["20260101"])],
            vec![],
            "source-b",
            HashMap::from([(
                ImportedStationId::from("Y".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("wikidata".to_string()),
                    Some("wikipedia".to_string()),
                ),
            )]),
        );
        repo.import_timetable(source_b);

        let cities = repo.all_cities();
        assert_eq!(
            cities.len(),
            1,
            "Both sources with the same import_key should share one city row"
        );

        let mapping = repo.stations_to_city();
        assert_eq!(mapping.len(), 2);
        let city_for_x = mapping.get(&InternalStationId::from(1));
        let city_for_y = mapping.get(&InternalStationId::from(2));
        assert_eq!(
            city_for_x, city_for_y,
            "Stations from different sources with the same import_key should map to the same city"
        );
    }

    // ---- wikidata/wikipedia field tests ----

    #[test]
    fn test_wikidata_and_wikipedia_are_stored() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("Q90".to_string()),
                    Some("fr:Paris".to_string()),
                ),
            )]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        assert_eq!(cities.len(), 1);
        let city_id = cities[0].id();

        let metadata = repo
            .get_city_metadata(**city_id)
            .expect("City metadata should exist");
        assert_eq!(metadata.0, Some("Q90".to_string()));
        assert_eq!(metadata.1, Some("fr:Paris".to_string()));
    }

    #[test]
    fn test_wikidata_and_wikipedia_can_be_none() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "SmallTown".into(),
                    "France".into(),
                    45.0,
                    1.0,
                    "key-smalltown".into(),
                    None,
                    None,
                ),
            )]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        assert_eq!(cities.len(), 1);
        let city_id = cities[0].id();

        let metadata = repo
            .get_city_metadata(**city_id)
            .expect("City metadata should exist");
        assert_eq!(metadata.0, None);
        assert_eq!(metadata.1, None);
    }

    #[test]
    fn test_only_wikidata_present() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Lyon".into(),
                    "France".into(),
                    45.75,
                    4.85,
                    "key-lyon".into(),
                    Some("Q456".to_string()),
                    None,
                ),
            )]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        let city_id = cities[0].id();
        let metadata = repo.get_city_metadata(**city_id).unwrap();
        assert_eq!(metadata.0, Some("Q456".to_string()));
        assert_eq!(metadata.1, None);
    }

    #[test]
    fn test_only_wikipedia_present() {
        let mut repo = make_repo();
        let data = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Marseille".into(),
                    "France".into(),
                    43.3,
                    5.4,
                    "key-marseille".into(),
                    None,
                    Some("fr:Marseille".to_string()),
                ),
            )]),
        );

        repo.import_timetable(data);

        let cities = repo.all_cities();
        let city_id = cities[0].id();
        let metadata = repo.get_city_metadata(**city_id).unwrap();
        assert_eq!(metadata.0, None);
        assert_eq!(metadata.1, Some("fr:Marseille".to_string()));
    }

    #[test]
    fn test_upsert_updates_wikidata_and_wikipedia() {
        let mut repo = make_repo();

        // First import with some metadata
        let first = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("Q90_old".to_string()),
                    Some("en:Paris_old".to_string()),
                ),
            )]),
        );
        repo.import_timetable(first);

        let cities = repo.all_cities();
        let city_id = cities[0].id();
        let metadata_before = repo.get_city_metadata(**city_id).unwrap();
        assert_eq!(metadata_before.0, Some("Q90_old".to_string()));
        assert_eq!(metadata_before.1, Some("en:Paris_old".to_string()));

        // Second import with updated metadata
        let second = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("Q90".to_string()),
                    Some("fr:Paris".to_string()),
                ),
            )]),
        );
        repo.import_timetable(second);

        let cities = repo.all_cities();
        assert_eq!(
            cities.len(),
            1,
            "Should still have only one city after upsert"
        );
        let city_id = cities[0].id();
        let metadata_after = repo.get_city_metadata(**city_id).unwrap();
        assert_eq!(
            metadata_after.0,
            Some("Q90".to_string()),
            "Wikidata should be updated"
        );
        assert_eq!(
            metadata_after.1,
            Some("fr:Paris".to_string()),
            "Wikipedia should be updated"
        );
    }

    #[test]
    fn test_upsert_can_clear_wikidata_and_wikipedia() {
        let mut repo = make_repo();

        // First import with metadata
        let first = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    Some("Q90".to_string()),
                    Some("fr:Paris".to_string()),
                ),
            )]),
        );
        repo.import_timetable(first);

        // Second import with None values
        let second = data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    None,
                    None,
                ),
            )]),
        );
        repo.import_timetable(second);

        let cities = repo.all_cities();
        let city_id = cities[0].id();
        let metadata = repo.get_city_metadata(**city_id).unwrap();
        assert_eq!(
            metadata.0, None,
            "Wikidata should be cleared on upsert with None"
        );
        assert_eq!(
            metadata.1, None,
            "Wikipedia should be cleared on upsert with None"
        );
    }

    // ---- all_cities ----

    fn two_city_repo() -> SqliteRepository {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Paris".into(),
                        "France".into(),
                        48.8566,
                        2.3522,
                        "key-paris".into(),
                        None,
                        None,
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "London".into(),
                        "UK".into(),
                        51.5074,
                        -0.1278,
                        "key-london".into(),
                        None,
                        None,
                    ),
                ),
            ]),
        ));
        repo
    }

    #[test]
    fn all_cities_returns_empty_when_no_data() {
        let repo = make_repo();
        assert!(repo.all_cities().is_empty());
    }

    #[test]
    fn all_cities_returns_all_cities() {
        let repo = two_city_repo();
        let cities = repo.all_cities();
        assert_eq!(cities.len(), 2);
        assert!(cities.iter().any(|c| *c.name() == "Paris".into()));
        assert!(cities.iter().any(|c| *c.name() == "London".into()));
    }

    #[test]
    fn all_cities_returns_cities_ordered_by_id() {
        let repo = two_city_repo();
        let cities = repo.all_cities();
        for i in 1..cities.len() {
            assert!(cities[i - 1].id() <= cities[i].id());
        }
    }

    // ---- search_cities_by_name ----

    #[test]
    fn search_cities_by_name_returns_empty_when_no_match() {
        let repo = two_city_repo();
        let results = repo.search_cities_by_name("Berlin", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn search_cities_by_name_matches_exact_name() {
        let repo = two_city_repo();
        let results = repo.search_cities_by_name("Paris", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(*results[0].name(), "Paris".into());
    }

    #[test]
    fn search_cities_by_name_is_case_insensitive() {
        let repo = two_city_repo();
        let results = repo.search_cities_by_name("paris", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(*results[0].name(), "Paris".into());
    }

    #[test]
    fn search_cities_by_name_matches_partial_name() {
        let repo = two_city_repo();
        let results = repo.search_cities_by_name("ari", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(*results[0].name(), "Paris".into());
    }

    #[test]
    fn search_cities_by_name_respects_limit() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B"), station("C")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Amsterdam".into(),
                        "NL".into(),
                        52.37,
                        4.9,
                        "key-ams".into(),
                        None,
                        None,
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "Antwerp".into(),
                        "BE".into(),
                        51.22,
                        4.4,
                        "key-ant".into(),
                        None,
                        None,
                    ),
                ),
                (
                    ImportedStationId::from("C".to_string()),
                    CityInformation::new(
                        "Alicante".into(),
                        "ES".into(),
                        38.35,
                        -0.48,
                        "key-ali".into(),
                        None,
                        None,
                    ),
                ),
            ]),
        ));

        let results = repo.search_cities_by_name("a", 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn search_cities_by_name_returns_results_ordered_alphabetically() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B"), station("C")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([
                (
                    ImportedStationId::from("A".to_string()),
                    CityInformation::new(
                        "Zurich".into(),
                        "CH".into(),
                        47.37,
                        8.54,
                        "key-zur".into(),
                        None,
                        None,
                    ),
                ),
                (
                    ImportedStationId::from("B".to_string()),
                    CityInformation::new(
                        "Athens".into(),
                        "GR".into(),
                        37.98,
                        23.73,
                        "key-ath".into(),
                        None,
                        None,
                    ),
                ),
                (
                    ImportedStationId::from("C".to_string()),
                    CityInformation::new(
                        "Madrid".into(),
                        "ES".into(),
                        40.42,
                        -3.7,
                        "key-mad".into(),
                        None,
                        None,
                    ),
                ),
            ]),
        ));

        let results = repo.search_cities_by_name("", 10);
        assert_eq!(results.len(), 3);
        assert_eq!(*results[0].name(), "Athens".into());
        assert_eq!(*results[1].name(), "Madrid".into());
        assert_eq!(*results[2].name(), "Zurich".into());
    }

    // ---- create_label ----

    #[test]
    fn create_label_returns_id() {
        let mut repo = make_repo();
        let id = repo.create_label("Capital".into());
        assert!(id.is_ok());
    }

    #[test]
    fn create_label_different_names_get_different_ids() {
        let mut repo = make_repo();
        let id1 = repo.create_label("Capital".into()).unwrap();
        let id2 = repo.create_label("Hub".into()).unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn create_label_duplicate_name_returns_error() {
        let mut repo = make_repo();
        repo.create_label("Capital".into()).unwrap();
        let err = repo.create_label("Capital".into());
        assert!(matches!(
            err,
            Err(LabelCreationError::LabelNameAlreadyExists)
        ));
    }

    // ---- add_label_to_city ----

    fn repo_with_city() -> (SqliteRepository, CityId) {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A")],
            vec![schedule("S1", &["20260101"])],
            vec![],
            "source",
            HashMap::from([(
                ImportedStationId::from("A".to_string()),
                CityInformation::new(
                    "Paris".into(),
                    "France".into(),
                    48.8566,
                    2.3522,
                    "key-paris".into(),
                    None,
                    None,
                ),
            )]),
        ));
        let city_id = *repo.all_cities()[0].id();
        (repo, city_id)
    }

    #[test]
    fn add_label_to_city_succeeds() {
        let (mut repo, city_id) = repo_with_city();
        let label_id = repo.create_label("Capital".into()).unwrap();
        assert!(repo.add_label_to_city(&city_id, &label_id).is_ok());
    }

    #[test]
    fn add_label_to_city_is_idempotent() {
        let (mut repo, city_id) = repo_with_city();
        let label_id = repo.create_label("Capital".into()).unwrap();
        repo.add_label_to_city(&city_id, &label_id).unwrap();
        assert!(repo.add_label_to_city(&city_id, &label_id).is_ok());
    }

    #[test]
    fn add_label_to_city_nonexistent_city_returns_error() {
        let mut repo = make_repo();
        let label_id = repo.create_label("Capital".into()).unwrap();
        let err = repo.add_label_to_city(&CityId::from(9999), &label_id);
        assert!(matches!(err, Err(AddLabelToCityError::CityNotFound)));
    }

    #[test]
    fn add_label_to_city_nonexistent_label_returns_error() {
        let (mut repo, city_id) = repo_with_city();
        let err = repo.add_label_to_city(&city_id, &CityLabelId::from(9999));
        assert!(matches!(err, Err(AddLabelToCityError::LabelNotFound)));
    }
}
