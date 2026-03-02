use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, Result, Transaction, params};

use crate::app::schedule::{
    ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
    ImportedTripLeg, InternalStation, InternalStationId, RemapError, StationChange, StationMapping,
    TimetableImportResult, TrainDataRepository, TrainDataToImport,
};

/// SQLite-backed implementation of [`TrainDataRepository`].
///
/// The database is initialised with [`SqliteRepository::open`] (file) or
/// [`SqliteRepository::open_in_memory`] (fully in-process, useful in tests).
/// The schema is created on construction and is idempotent (uses `CREATE TABLE IF NOT EXISTS`).
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
        let repo = Self { conn };
        repo.init_schema()?;
        Ok(repo)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS stations (
                source TEXT NOT NULL,
                id     TEXT PRIMARY KEY,
                name   TEXT NOT NULL,
                lat    REAL NOT NULL,
                lon    REAL NOT NULL
            );

            -- dates are stored as a comma-separated list of YYYYMMDD strings
            CREATE TABLE IF NOT EXISTS schedules (
                source TEXT NOT NULL,
                id     TEXT PRIMARY KEY,
                dates  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trips (
                source      TEXT    NOT NULL,
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                route       TEXT    NOT NULL,
                origin      TEXT    NOT NULL,
                destination TEXT    NOT NULL,
                departure   INTEGER NOT NULL,
                arrival     INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS route_schedules (
                source      TEXT NOT NULL,
                route_id    TEXT NOT NULL,
                schedule_id TEXT NOT NULL,
                PRIMARY KEY (source, route_id, schedule_id)
            );

            -- canonical, source-agnostic stations
            CREATE TABLE IF NOT EXISTS internal_stations (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                lat  REAL NOT NULL,
                lon  REAL NOT NULL
            );

            -- maps every (source, source_id) pair to one internal_station
            CREATE TABLE IF NOT EXISTS station_mappings (
                source      TEXT    NOT NULL,
                source_id   TEXT    NOT NULL,
                internal_id INTEGER NOT NULL REFERENCES internal_stations(id),
                PRIMARY KEY (source, source_id)
            );
            ",
        )
    }
}

impl SqliteRepository {
    /// Load all currently persisted stations, keyed by their string id.
    fn load_existing_stations(tx: &Transaction) -> HashMap<String, ImportedStation> {
        let mut stmt = tx
            .prepare_cached("SELECT id, name, lat, lon FROM stations")
            .expect("load_existing_stations: prepare failed");
        stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let lat: f64 = row.get(2)?;
            let lon: f64 = row.get(3)?;
            Ok((
                id.clone(),
                ImportedStation::new(ImportedStationId::from(id), name, lat, lon),
            ))
        })
        .expect("load_existing_stations: query failed")
        .map(|r| r.expect("load_existing_stations: row failed"))
        .collect()
    }

    /// Diff incoming stations against what is already stored and return
    /// the list of additions and updates (unchanged stations are omitted).
    fn diff_stations(
        existing: &HashMap<String, ImportedStation>,
        incoming: &[ImportedStation],
    ) -> Vec<StationChange> {
        incoming
            .iter()
            .filter_map(|s| match existing.get(s.id().as_str()) {
                None => Some(StationChange::Added(s.clone())),
                Some(old) if old != s => Some(StationChange::Updated(s.clone())),
                _ => None,
            })
            .collect()
    }

    /// Delete all rows from the three volatile tables in a single batch.
    fn truncate_timetable(tx: &Transaction, source: &str) {
        tx.prepare_cached("DELETE FROM route_schedules WHERE source = ?1;")
            .expect("delete_from_route_schedules: prepare failed")
            .execute(params![source])
            .expect("delete_from_route_schedules: execute failed");

        tx.prepare_cached("DELETE FROM schedules WHERE source = ?1;")
            .expect("delete_schedules: prepare failed")
            .execute(params![source])
            .expect("delete_schedules: execute failed");

        tx.prepare_cached("DELETE FROM trips WHERE source = ?1;")
            .expect("delete_from_trips: prepare failed")
            .execute(params![source])
            .expect("delete_from_trips: execute failed");
    }

    /// Upsert stations; existing rows are overwritten, new ones inserted.
    fn upsert_stations(tx: &Transaction, stations: &[ImportedStation], source: &str) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT OR REPLACE INTO stations (id, name, source, lat, lon)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .expect("upsert_stations: prepare failed");
        for s in stations {
            stmt.execute(params![s.id().as_str(), s.name(), source, s.lat(), s.lon()])
                .expect("upsert_stations: execute failed");
        }
    }

    /// Insert schedules (table was just truncated, so no conflict is expected).
    fn insert_schedules(tx: &Transaction, schedules: &[ImportedSchedule], source: &str) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO schedules (id, source, dates)
                 VALUES (?1, ?2, ?3)",
            )
            .expect("insert_schedules: prepare failed");
        for s in schedules {
            let dates = s.dates().join(",");
            stmt.execute(params![s.id().as_str(), source, dates])
                .expect("insert_schedules: execute failed");
        }
    }

    /// Insert trip legs (table was just truncated, so no conflict is expected).
    fn insert_trips(tx: &Transaction, trips: &[ImportedTripLeg], source: &str) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO trips (route, source, origin, destination, departure, arrival)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .expect("insert_trips: prepare failed");
        for t in trips {
            stmt.execute(params![
                t.route().as_str(),
                source,
                t.origin().as_str(),
                t.destination().as_str(),
                t.departure() as i64,
                t.arrival() as i64,
            ])
            .expect("insert_trips: execute failed");
        }
    }

    /// Load all existing (source, source_id) pairs that already have a mapping.
    fn load_mapped_keys(tx: &Transaction) -> HashSet<(String, String)> {
        let mut stmt = tx
            .prepare_cached("SELECT source, source_id FROM station_mappings")
            .expect("load_mapped_keys: prepare failed");
        stmt.query_map([], |row| {
            let source: String = row.get(0)?;
            let source_id: String = row.get(1)?;
            Ok((source, source_id))
        })
        .expect("load_mapped_keys: query failed")
        .map(|r| r.expect("load_mapped_keys: row failed"))
        .collect()
    }

    /// Insert a new internal station derived from a source station and return its id.
    fn insert_internal_station(tx: &Transaction, station: &ImportedStation) -> InternalStationId {
        tx.prepare_cached("INSERT INTO internal_stations (name, lat, lon) VALUES (?1, ?2, ?3)")
            .expect("insert_internal_station: prepare failed")
            .execute(params![station.name(), station.lat(), station.lon()])
            .expect("insert_internal_station: execute failed");
        InternalStationId::from(tx.last_insert_rowid())
    }

    /// Record that `(source, source_id)` maps to `internal_id`.
    fn insert_station_mapping(
        tx: &Transaction,
        source: &str,
        source_id: &ImportedStationId,
        internal_id: &InternalStationId,
    ) {
        tx.prepare_cached(
            "INSERT INTO station_mappings (source, source_id, internal_id)
             VALUES (?1, ?2, ?3)",
        )
        .expect("insert_station_mapping: prepare failed")
        .execute(params![source, source_id.as_str(), internal_id.as_i64()])
        .expect("insert_station_mapping: execute failed");
    }

    /// Insert route–schedule mappings (table was just truncated).
    fn insert_route_schedules(
        tx: &Transaction,
        mapping: &HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
        source: &str,
    ) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO route_schedules (source, route_id, schedule_id)
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
}

impl TrainDataRepository for SqliteRepository {
    fn import_timetable(&mut self, data: TrainDataToImport) -> TimetableImportResult {
        let tx = self
            .conn
            .transaction()
            .expect("import_timetable: begin transaction failed");

        let existing = Self::load_existing_stations(&tx);
        let station_changes = Self::diff_stations(&existing, data.stations());

        // Determine which source stations already have an internal mapping.
        let mapped_keys = Self::load_mapped_keys(&tx);

        Self::truncate_timetable(&tx, data.source());
        Self::upsert_stations(&tx, data.stations(), data.source());
        Self::insert_schedules(&tx, data.schedules(), data.source());
        Self::insert_trips(&tx, data.trip_legs(), data.source());
        Self::insert_route_schedules(&tx, data.schedules_by_route(), data.source());

        // For every source station without an existing mapping, create a fresh
        // internal station and link it.  Existing mappings are never modified.
        let mut new_internal_stations = Vec::new();
        for s in data.stations() {
            let key = (data.source().to_owned(), s.id().as_str().to_owned());
            if !mapped_keys.contains(&key) {
                let internal_id = Self::insert_internal_station(&tx, s);
                Self::insert_station_mapping(&tx, data.source(), s.id(), &internal_id);
                new_internal_stations.push(internal_id);
            }
        }

        tx.commit().expect("import_timetable: commit failed");
        println!(
            "import_timetable: {} stations ({} changes, {} new internal), {} schedules, {} trips",
            data.stations().len(),
            station_changes.len(),
            new_internal_stations.len(),
            data.schedules().len(),
            data.trip_legs().len(),
        );

        TimetableImportResult {
            station_changes,
            new_internal_stations,
        }
    }
    fn all_stations(&self) -> Vec<ImportedStation> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, lat, lon FROM stations")
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

    fn all_schedules(&self) -> Vec<ImportedSchedule> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, dates FROM schedules")
            .expect("all_schedules: prepare failed");

        stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let dates_raw: String = row.get(1)?;
            let dates: Vec<String> = if dates_raw.is_empty() {
                vec![]
            } else {
                dates_raw.split(',').map(str::to_owned).collect()
            };
            Ok(ImportedSchedule::new(ImportedScheduleId::from(id), dates))
        })
        .expect("all_schedules: query failed")
        .map(|r| r.expect("all_schedules: row mapping failed"))
        .collect()
    }

    fn all_trips(&self) -> Vec<ImportedTripLeg> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT route, origin, destination, departure, arrival
                 FROM trips",
            )
            .expect("all_trips: prepare failed");

        stmt.query_map([], |row| {
            let route: String = row.get(0)?;
            let origin: String = row.get(1)?;
            let destination: String = row.get(2)?;
            let departure: i64 = row.get(3)?;
            let arrival: i64 = row.get(4)?;
            Ok(ImportedTripLeg::new(
                ImportedRouteId::from(route),
                ImportedStationId::from(origin),
                ImportedStationId::from(destination),
                departure as usize,
                arrival as usize,
            ))
        })
        .expect("all_trips: query failed")
        .map(|r| r.expect("all_trips: row mapping failed"))
        .collect()
    }

    fn schedules_by_route(&self) -> HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
        let mut stmt = self
            .conn
            .prepare("SELECT route_id, schedule_id FROM route_schedules")
            .expect("schedules_by_route: prepare failed");

        let mut map: HashMap<ImportedRouteId, Vec<ImportedScheduleId>> = HashMap::new();
        stmt.query_map([], |row| {
            let route: String = row.get(0)?;
            let schedule: String = row.get(1)?;
            Ok((route, schedule))
        })
        .expect("schedules_by_route: query failed")
        .map(|r| r.expect("schedules_by_route: row mapping failed"))
        .for_each(|(route, schedule)| {
            map.entry(ImportedRouteId::from(route))
                .or_default()
                .push(ImportedScheduleId::from(schedule));
        });

        map
    }

    fn internal_stations(&self) -> Vec<InternalStation> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, lat, lon FROM internal_stations")
            .expect("internal_stations: prepare failed");

        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let lat: f64 = row.get(2)?;
            let lon: f64 = row.get(3)?;
            Ok(InternalStation::new(
                InternalStationId::from(id),
                name,
                lat,
                lon,
            ))
        })
        .expect("internal_stations: query failed")
        .map(|r| r.expect("internal_stations: row mapping failed"))
        .collect()
    }

    fn search_internal_stations_by_name(&self, query: &str, limit: usize) -> Vec<InternalStation> {
        let pattern = format!("%{}%", query);
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, name, lat, lon FROM internal_stations
                 WHERE LOWER(name) LIKE LOWER(?1)
                 ORDER BY name
                 LIMIT ?2",
            )
            .expect("search_internal_stations_by_name: prepare failed");

        stmt.query_map(params![pattern, limit as i64], |row| {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let lat: f64 = row.get(2)?;
            let lon: f64 = row.get(3)?;
            Ok(InternalStation::new(
                InternalStationId::from(id),
                name,
                lat,
                lon,
            ))
        })
        .expect("search_internal_stations_by_name: query failed")
        .map(|r| r.expect("search_internal_stations_by_name: row mapping failed"))
        .collect()
    }

    fn station_mappings(&self) -> Vec<StationMapping> {
        let mut stmt = self
            .conn
            .prepare("SELECT source, source_id, internal_id FROM station_mappings")
            .expect("station_mappings: prepare failed");

        stmt.query_map([], |row| {
            let source: String = row.get(0)?;
            let source_id: String = row.get(1)?;
            let internal_id: i64 = row.get(2)?;
            Ok(StationMapping {
                source,
                source_id: ImportedStationId::from(source_id),
                internal_id: InternalStationId::from(internal_id),
            })
        })
        .expect("station_mappings: query failed")
        .map(|r| r.expect("station_mappings: row mapping failed"))
        .collect()
    }

    fn remap_station(
        &mut self,
        source: &str,
        source_id: &ImportedStationId,
        new_internal_id: &InternalStationId,
    ) -> Result<(), RemapError> {
        let tx = self
            .conn
            .transaction()
            .expect("remap_station: begin transaction failed");

        // Verify the target internal station exists.
        let exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM internal_stations WHERE id = ?1",
                params![new_internal_id.as_i64()],
                |row| row.get::<_, i64>(0),
            )
            .expect("remap_station: existence check failed")
            > 0;

        if !exists {
            return Err(RemapError::InternalStationNotFound);
        }

        // Fetch the current internal_id (and confirm the mapping exists).
        let old_internal_id: Option<i64> = tx
            .query_row(
                "SELECT internal_id FROM station_mappings
                 WHERE source = ?1 AND source_id = ?2",
                params![source, source_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .expect("remap_station: old id fetch failed");

        let old_internal_id = match old_internal_id {
            Some(id) => id,
            None => return Err(RemapError::MappingNotFound),
        };

        // Update the mapping.
        tx.prepare_cached(
            "UPDATE station_mappings
             SET internal_id = ?3
             WHERE source = ?1 AND source_id = ?2",
        )
        .expect("remap_station: prepare failed")
        .execute(params![
            source,
            source_id.as_str(),
            new_internal_id.as_i64()
        ])
        .expect("remap_station: execute failed");

        // Delete the old internal station if it is now unreferenced.
        if old_internal_id != new_internal_id.as_i64() {
            let still_referenced: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM station_mappings WHERE internal_id = ?1",
                    params![old_internal_id],
                    |row| row.get(0),
                )
                .expect("remap_station: reference count failed");

            if still_referenced == 0 {
                tx.prepare_cached("DELETE FROM internal_stations WHERE id = ?1")
                    .expect("remap_station: delete prepare failed")
                    .execute(params![old_internal_id])
                    .expect("remap_station: delete execute failed");
            }
        }

        tx.commit().expect("remap_station: commit failed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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

    fn station_moved(id: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            9.0,
            9.0,
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
    ) -> TrainDataToImport {
        let mut sbr = std::collections::HashMap::new();
        for s in &schedules {
            sbr.entry(ImportedRouteId::from("R1".to_owned()))
                .or_insert_with(Vec::new)
                .push(s.id().clone());
        }
        TrainDataToImport::new(stations, trips, schedules, sbr, source.to_owned())
    }

    // ---- round-trip tests ----

    #[test]
    fn round_trip_stations() {
        let mut repo = make_repo();
        let input = vec![station("A"), station("B")];
        repo.import_timetable(data_to_import(input.clone(), vec![], vec![], "source"));
        let mut result = repo.all_stations();
        result.sort_by_key(|s| s.id().as_str().to_owned());
        assert_eq!(result, input);
    }

    #[test]
    fn round_trip_schedules() {
        let mut repo = make_repo();
        let sched = vec![schedule("S1", &["20260101", "20260102"])];
        repo.import_timetable(data_to_import(vec![], sched.clone(), vec![], "source"));
        assert_eq!(repo.all_schedules(), sched);
    }

    #[test]
    fn round_trip_trips() {
        let mut repo = make_repo();
        let trips = vec![trip("R1", "A", "B", 100, 200)];
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![],
            trips.clone(),
            "source",
        ));
        assert_eq!(repo.all_trips(), trips);
    }

    #[test]
    fn round_trip_schedules_by_route() {
        let mut repo = make_repo();
        let sched = vec![schedule("S1", &["20260101"])];
        let data = data_to_import(vec![], sched, vec![], "source");
        repo.import_timetable(data);
        let result = repo.schedules_by_route();
        assert!(result.contains_key(&ImportedRouteId::from("R1".to_owned())));
    }

    // ---- successive import tests ----

    #[test]
    fn successive_import_replaces_timetable() {
        let mut repo = make_repo();

        let first = data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
        );
        repo.import_timetable(first);

        let second = data_to_import(
            vec![station("A"), station("C")],
            vec![schedule("S2", &["20260201"])],
            vec![trip("R2", "A", "C", 300, 400)],
            "source",
        );
        repo.import_timetable(second);

        // Volatile data replaced
        let trips = repo.all_trips();
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].route().as_str(), "R2");

        let schedules = repo.all_schedules();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].id().as_str(), "S2");

        // Station B from first import is retained; C added
        let mut stations = repo.all_stations();
        stations.sort_by_key(|s| s.id().as_str().to_owned());
        assert_eq!(
            stations.iter().map(|s| s.id().as_str()).collect::<Vec<_>>(),
            ["A", "B", "C"]
        );
    }

    // ---- station diff tests ----

    #[test]
    fn import_reports_new_stations() {
        let mut repo = make_repo();
        let snap = data_to_import(vec![station("A"), station("B")], vec![], vec![], "source");
        let result = repo.import_timetable(snap);
        let added: Vec<_> = result
            .station_changes
            .iter()
            .filter_map(|c| match c {
                StationChange::Added(s) => Some(s.id().as_str()),
                _ => None,
            })
            .collect();
        assert!(added.contains(&"A"));
        assert!(added.contains(&"B"));
    }

    #[test]
    fn import_reports_updated_stations() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "source"));

        // Second import with updated coordinates
        let result = repo.import_timetable(data_to_import(
            vec![station_moved("A")],
            vec![],
            vec![],
            "source",
        ));
        let updated: Vec<_> = result
            .station_changes
            .iter()
            .filter_map(|c| match c {
                StationChange::Updated(s) => Some(s.id().as_str()),
                _ => None,
            })
            .collect();
        assert!(updated.contains(&"A"));
    }

    #[test]
    fn unchanged_stations_produce_no_diff() {
        let mut repo = make_repo();
        let snap = data_to_import(vec![station("A")], vec![], vec![], "source");
        repo.import_timetable(snap.clone());
        let result = repo.import_timetable(snap);
        assert!(result.station_changes.is_empty());
    }

    // ---- diff_stations: additional cases ----

    #[test]
    fn station_name_change_is_reported_as_updated() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "source"));

        let renamed = ImportedStation::new(
            ImportedStationId::from("A".to_owned()),
            "Renamed".to_owned(),
            1.0,
            2.0,
        );
        let result = repo.import_timetable(data_to_import(vec![renamed], vec![], vec![], "source"));
        let updated: Vec<_> = result
            .station_changes
            .iter()
            .filter_map(|c| match c {
                StationChange::Updated(s) => Some(s.id().as_str()),
                _ => None,
            })
            .collect();
        assert!(updated.contains(&"A"));
    }

    #[test]
    fn station_absent_from_re_import_is_silently_retained() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![],
            vec![],
            "source",
        ));

        // Second import only mentions A — B should still be in the DB.
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "source"));
        let ids: Vec<_> = repo
            .all_stations()
            .into_iter()
            .map(|s| s.id().as_str().to_owned())
            .collect();
        assert!(ids.contains(&"B".to_owned()));
    }

    // ---- truncate_timetable ----

    #[test]
    fn empty_second_import_clears_volatile_tables() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
        ));

        // Second import is empty for timetable data.
        repo.import_timetable(data_to_import(vec![], vec![], vec![], "source"));

        assert!(repo.all_trips().is_empty(), "trips should be empty");
        assert!(repo.all_schedules().is_empty(), "schedules should be empty");
        assert!(
            repo.schedules_by_route().is_empty(),
            "route_schedules should be empty"
        );
    }

    // ---- import_timetable with fully empty data ----

    #[test]
    fn fully_empty_import_does_not_panic_and_returns_no_changes() {
        let mut repo = make_repo();
        let result = repo.import_timetable(data_to_import(vec![], vec![], vec![], "source"));
        assert!(result.station_changes.is_empty());
        assert!(repo.all_stations().is_empty());
        assert!(repo.all_trips().is_empty());
        assert!(repo.all_schedules().is_empty());
    }

    // ---- all_schedules edge cases ----

    #[test]
    fn schedule_with_no_dates_round_trips_correctly() {
        let mut repo = make_repo();
        let empty_sched = ImportedSchedule::new(ImportedScheduleId::from("S0".to_owned()), vec![]);
        repo.import_timetable(data_to_import(
            vec![],
            vec![empty_sched.clone()],
            vec![],
            "source",
        ));
        let result = repo.all_schedules();
        assert_eq!(result, vec![empty_sched]);
    }

    // ---- schedules_by_route ----

    #[test]
    fn route_with_multiple_schedules_round_trips_correctly() {
        let mut repo = make_repo();
        let s1 = schedule("S1", &["20260101"]);
        let s2 = schedule("S2", &["20260201"]);
        let mut sbr = HashMap::new();
        sbr.insert(
            ImportedRouteId::from("R1".to_owned()),
            vec![
                ImportedScheduleId::from("S1".to_owned()),
                ImportedScheduleId::from("S2".to_owned()),
            ],
        );
        let snap = TrainDataToImport::new(vec![], vec![], vec![s1, s2], sbr, "source".to_owned());
        repo.import_timetable(snap);
        let result = repo.schedules_by_route();
        let mut ids: Vec<_> = result
            .get(&ImportedRouteId::from("R1".to_owned()))
            .expect("R1 should be present")
            .iter()
            .map(|s| s.as_str().to_owned())
            .collect();
        ids.sort();
        assert_eq!(ids, ["S1", "S2"]);
    }

    // ---- all_trips ----

    #[test]
    fn multiple_trips_all_persisted() {
        let mut repo = make_repo();
        let trips = vec![
            trip("R1", "A", "B", 100, 200),
            trip("R1", "B", "C", 210, 300),
            trip("R2", "A", "C", 400, 500),
        ];
        repo.import_timetable(data_to_import(
            vec![station("A"), station("B"), station("C")],
            vec![],
            trips.clone(),
            "source",
        ));
        let mut result = repo.all_trips();
        result.sort();
        let mut expected = trips;
        expected.sort();
        assert_eq!(result, expected);
    }

    // ---- internal stations and station mappings ----

    #[test]
    fn import_creates_internal_station_for_new_source_station() {
        let mut repo = make_repo();
        let result = repo.import_timetable(data_to_import(
            vec![station("A"), station("B")],
            vec![],
            vec![],
            "db",
        ));
        assert_eq!(result.new_internal_stations.len(), 2);
        assert_eq!(repo.internal_stations().len(), 2);
        assert_eq!(repo.station_mappings().len(), 2);
    }

    #[test]
    fn reimport_does_not_create_duplicate_internal_stations() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));
        // Second import of the same source station must not create a new internal station.
        let result =
            repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));
        assert!(result.new_internal_stations.is_empty());
        assert_eq!(repo.internal_stations().len(), 1);
        assert_eq!(repo.station_mappings().len(), 1);
    }

    #[test]
    fn same_physical_station_from_two_sources_gets_two_internal_stations_by_default() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(
            vec![station("330323")],
            vec![],
            vec![],
            "db",
        ));
        repo.import_timetable(data_to_import(
            vec![station("StopArea:OCE87113001")],
            vec![],
            vec![],
            "sncf",
        ));
        // Without a manual merge the two sources each get their own internal station.
        assert_eq!(repo.internal_stations().len(), 2);
        assert_eq!(repo.station_mappings().len(), 2);
    }

    #[test]
    fn station_mappings_round_trip() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));
        let mappings = repo.station_mappings();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].source, "db");
        assert_eq!(
            mappings[0].source_id,
            ImportedStationId::from("A".to_owned())
        );
        assert_eq!(
            mappings[0].internal_id,
            repo.internal_stations()[0].id().clone()
        );
    }

    #[test]
    fn empty_import_creates_no_internal_stations() {
        let mut repo = make_repo();
        let result = repo.import_timetable(data_to_import(vec![], vec![], vec![], "db"));
        assert!(result.new_internal_stations.is_empty());
        assert!(repo.internal_stations().is_empty());
        assert!(repo.station_mappings().is_empty());
    }

    // ---- remap_station ----

    #[test]
    fn remap_station_updates_mapping() {
        let mut repo = make_repo();
        // Import two sources; each gets its own internal station.
        repo.import_timetable(data_to_import(
            vec![station("330323")],
            vec![],
            vec![],
            "db",
        ));
        repo.import_timetable(data_to_import(
            vec![station("StopArea:OCE87113001")],
            vec![],
            vec![],
            "sncf",
        ));

        let mappings = repo.station_mappings();
        let db_internal = mappings
            .iter()
            .find(|m| m.source == "db")
            .unwrap()
            .internal_id
            .clone();
        let sncf_source_id = ImportedStationId::from("StopArea:OCE87113001".to_owned());

        // Merge: point the sncf station at the db internal station.
        repo.remap_station("sncf", &sncf_source_id, &db_internal)
            .expect("remap should succeed");

        let updated = repo.station_mappings();
        let sncf_mapping = updated.iter().find(|m| m.source == "sncf").unwrap();
        assert_eq!(sncf_mapping.internal_id, db_internal);
        // The sncf-only internal station is now orphaned and must be deleted.
        assert_eq!(repo.internal_stations().len(), 1);
    }

    #[test]
    fn remap_station_keeps_internal_station_when_still_referenced() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));
        repo.import_timetable(data_to_import(vec![station("X")], vec![], vec![], "sncf"));
        repo.import_timetable(data_to_import(vec![station("Y")], vec![], vec![], "fr"));

        let mappings = repo.station_mappings();
        let sncf_internal = mappings
            .iter()
            .find(|m| m.source == "sncf")
            .unwrap()
            .internal_id
            .clone();
        let db_internal = mappings
            .iter()
            .find(|m| m.source == "db")
            .unwrap()
            .internal_id
            .clone();

        // Point fr/Y at sncf's internal station (orphans fr's own → deleted).
        repo.remap_station(
            "fr",
            &ImportedStationId::from("Y".to_owned()),
            &sncf_internal,
        )
        .expect("first remap");
        assert_eq!(repo.internal_stations().len(), 2);

        // Now remap sncf/X → db's internal station.
        // sncf_internal is still referenced by fr/Y, so it must NOT be deleted.
        repo.remap_station(
            "sncf",
            &ImportedStationId::from("X".to_owned()),
            &db_internal,
        )
        .expect("second remap");
        assert_eq!(repo.internal_stations().len(), 2);
    }

    #[test]
    fn remap_station_returns_error_for_unknown_mapping() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));

        let internal_ids = repo.internal_stations();
        let valid_id = internal_ids[0].id().clone();
        let unknown_source_id = ImportedStationId::from("nonexistent".to_owned());

        let err = repo
            .remap_station("db", &unknown_source_id, &valid_id)
            .unwrap_err();
        assert_eq!(err, RemapError::MappingNotFound);
    }

    #[test]
    fn remap_station_returns_error_for_unknown_internal_station() {
        let mut repo = make_repo();
        repo.import_timetable(data_to_import(vec![station("A")], vec![], vec![], "db"));

        let source_id = ImportedStationId::from("A".to_owned());
        let ghost_id = InternalStationId::from(99999_i64);

        let err = repo.remap_station("db", &source_id, &ghost_id).unwrap_err();
        assert_eq!(err, RemapError::InternalStationNotFound);
    }

    // ---- search_internal_stations_by_name ----

    fn named_station(id: &str, name: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            name.to_owned(),
            1.0,
            2.0,
        )
    }

    fn import_named(repo: &mut SqliteRepository, id: &str, name: &str, source: &str) {
        repo.import_timetable(data_to_import(
            vec![named_station(id, name)],
            vec![],
            vec![],
            source,
        ));
    }

    #[test]
    fn search_returns_matching_stations() {
        let mut repo = make_repo();
        import_named(&mut repo, "1", "Paris Gare de Lyon", "sncf");
        import_named(&mut repo, "2", "Paris Nord", "sncf");
        import_named(&mut repo, "3", "Lyon Part-Dieu", "sncf");

        let results = repo.search_internal_stations_by_name("paris", 10);
        let names: Vec<_> = results.iter().map(|s| s.name()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"Paris Gare de Lyon"));
        assert!(names.contains(&"Paris Nord"));
    }

    #[test]
    fn search_is_case_insensitive() {
        let mut repo = make_repo();
        import_named(&mut repo, "1", "Bordeaux Saint-Jean", "sncf");

        assert_eq!(
            repo.search_internal_stations_by_name("BORDEAUX", 10).len(),
            1
        );
        assert_eq!(
            repo.search_internal_stations_by_name("bordeaux", 10).len(),
            1
        );
        assert_eq!(
            repo.search_internal_stations_by_name("Bordeaux", 10).len(),
            1
        );
    }

    #[test]
    fn search_returns_empty_when_no_match() {
        let mut repo = make_repo();
        import_named(&mut repo, "1", "Marseille Saint-Charles", "sncf");

        assert!(
            repo.search_internal_stations_by_name("Berlin", 10)
                .is_empty()
        );
    }

    #[test]
    fn search_respects_limit() {
        let mut repo = make_repo();
        import_named(&mut repo, "1", "Gare A", "sncf");
        import_named(&mut repo, "2", "Gare B", "sncf");
        import_named(&mut repo, "3", "Gare C", "sncf");

        let results = repo.search_internal_stations_by_name("Gare", 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn search_results_are_ordered_alphabetically() {
        let mut repo = make_repo();
        import_named(&mut repo, "1", "Toulouse Matabiau", "sncf");
        import_named(&mut repo, "2", "Tours", "sncf");
        import_named(&mut repo, "3", "Toulon", "sncf");

        let results = repo.search_internal_stations_by_name("to", 10);
        let names: Vec<_> = results.iter().map(|s| s.name()).collect();
        assert_eq!(names, ["Toulon", "Toulouse Matabiau", "Tours"]);
    }

    #[test]
    fn search_on_empty_repository_returns_empty() {
        let repo = make_repo();
        assert!(
            repo.search_internal_stations_by_name("Paris", 10)
                .is_empty()
        );
    }
}
