use std::collections::HashMap;

use rusqlite::{Connection, Result, Transaction, params};

use crate::app::schedule::{
    ImportTrainData, ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation,
    ImportedStationId, ImportedTripLeg, StationChange, TimetableImportResult, TrainDataRepository,
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
                id   TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                lat  REAL NOT NULL,
                lon  REAL NOT NULL
            );

            -- dates are stored as a comma-separated list of YYYYMMDD strings
            CREATE TABLE IF NOT EXISTS schedules (
                id    TEXT PRIMARY KEY,
                dates TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trips (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                route       TEXT    NOT NULL,
                origin      TEXT    NOT NULL,
                destination TEXT    NOT NULL,
                departure   INTEGER NOT NULL,
                arrival     INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS route_schedules (
                route_id    TEXT NOT NULL,
                schedule_id TEXT NOT NULL,
                PRIMARY KEY (route_id, schedule_id)
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
    fn truncate_timetable(tx: &Transaction) {
        tx.execute_batch(
            "DELETE FROM route_schedules;
             DELETE FROM schedules;
             DELETE FROM trips;",
        )
        .expect("truncate_timetable: failed");
    }

    /// Upsert stations; existing rows are overwritten, new ones inserted.
    fn upsert_stations(tx: &Transaction, stations: &[ImportedStation]) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT OR REPLACE INTO stations (id, name, lat, lon)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .expect("upsert_stations: prepare failed");
        for s in stations {
            stmt.execute(params![s.id().as_str(), s.name(), s.lat(), s.lon()])
                .expect("upsert_stations: execute failed");
        }
    }

    /// Insert schedules (table was just truncated, so no conflict is expected).
    fn insert_schedules(tx: &Transaction, schedules: &[ImportedSchedule]) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO schedules (id, dates)
                 VALUES (?1, ?2)",
            )
            .expect("insert_schedules: prepare failed");
        for s in schedules {
            let dates = s.dates().join(",");
            stmt.execute(params![s.id().as_str(), dates])
                .expect("insert_schedules: execute failed");
        }
    }

    /// Insert trip legs (table was just truncated, so no conflict is expected).
    fn insert_trips(tx: &Transaction, trips: &[ImportedTripLeg]) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO trips (route, origin, destination, departure, arrival)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .expect("insert_trips: prepare failed");
        for t in trips {
            stmt.execute(params![
                t.route().as_str(),
                t.origin().as_str(),
                t.destination().as_str(),
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
    ) {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO route_schedules (route_id, schedule_id)
                 VALUES (?1, ?2)",
            )
            .expect("insert_route_schedules: prepare failed");
        for (route, schedules) in mapping {
            for schedule in schedules {
                stmt.execute(params![route.as_str(), schedule.as_str()])
                    .expect("insert_route_schedules: execute failed");
            }
        }
    }
}

impl TrainDataRepository for SqliteRepository {
    fn import_timetable<D: ImportTrainData>(&mut self, data: &D) -> TimetableImportResult {
        let tx = self
            .conn
            .transaction()
            .expect("import_timetable: begin transaction failed");

        let existing = Self::load_existing_stations(&tx);
        let station_changes = Self::diff_stations(&existing, data.stations());

        Self::truncate_timetable(&tx);
        Self::upsert_stations(&tx, data.stations());
        Self::insert_schedules(&tx, data.schedules());
        Self::insert_trips(&tx, data.trip_legs());
        Self::insert_route_schedules(&tx, data.schedules_by_route());

        tx.commit().expect("import_timetable: commit failed");
        println!(
            "import_timetable: {} stations ({} changes), {} schedules, {} trips",
            data.stations().len(),
            station_changes.len(),
            data.schedules().len(),
            data.trip_legs().len(),
        );

        TimetableImportResult { station_changes }
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

    #[derive(Clone)]
    struct Snapshot {
        stations: Vec<ImportedStation>,
        schedules: Vec<ImportedSchedule>,
        trips: Vec<ImportedTripLeg>,
        schedules_by_route: std::collections::HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
        source: String,
    }

    impl ImportTrainData for Snapshot {
        fn stations(&self) -> &[ImportedStation] {
            &self.stations
        }
        fn trip_legs(&self) -> &[ImportedTripLeg] {
            &self.trips
        }
        fn schedules(&self) -> &[ImportedSchedule] {
            &self.schedules
        }
        fn schedules_by_route(
            &self,
        ) -> &std::collections::HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
            &self.schedules_by_route
        }
        fn source(&self) -> &str {
            &self.source
        }
    }

    fn snapshot(
        stations: Vec<ImportedStation>,
        schedules: Vec<ImportedSchedule>,
        trips: Vec<ImportedTripLeg>,
        source: &str,
    ) -> Snapshot {
        let mut sbr = std::collections::HashMap::new();
        for s in &schedules {
            sbr.entry(ImportedRouteId::from("R1".to_owned()))
                .or_insert_with(Vec::new)
                .push(s.id().clone());
        }
        Snapshot {
            stations,
            schedules,
            trips,
            schedules_by_route: sbr,
            source: source.to_owned(),
        }
    }

    // ---- round-trip tests ----

    #[test]
    fn round_trip_stations() {
        let mut repo = make_repo();
        let input = vec![station("A"), station("B")];
        repo.import_timetable(&snapshot(input.clone(), vec![], vec![], "source"));
        let mut result = repo.all_stations();
        result.sort_by_key(|s| s.id().as_str().to_owned());
        assert_eq!(result, input);
    }

    #[test]
    fn round_trip_schedules() {
        let mut repo = make_repo();
        let sched = vec![schedule("S1", &["20260101", "20260102"])];
        repo.import_timetable(&snapshot(vec![], sched.clone(), vec![], "source"));
        assert_eq!(repo.all_schedules(), sched);
    }

    #[test]
    fn round_trip_trips() {
        let mut repo = make_repo();
        let trips = vec![trip("R1", "A", "B", 100, 200)];
        repo.import_timetable(&snapshot(
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
        let snap = snapshot(vec![], sched, vec![], "source");
        repo.import_timetable(&snap);
        let result = repo.schedules_by_route();
        assert!(
            result
                .get(&ImportedRouteId::from("R1".to_owned()))
                .is_some()
        );
    }

    // ---- successive import tests ----

    #[test]
    fn successive_import_replaces_timetable() {
        let mut repo = make_repo();

        let first = snapshot(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
        );
        repo.import_timetable(&first);

        let second = snapshot(
            vec![station("A"), station("C")],
            vec![schedule("S2", &["20260201"])],
            vec![trip("R2", "A", "C", 300, 400)],
            "source",
        );
        repo.import_timetable(&second);

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
        let snap = snapshot(vec![station("A"), station("B")], vec![], vec![], "source");
        let result = repo.import_timetable(&snap);
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
        repo.import_timetable(&snapshot(vec![station("A")], vec![], vec![], "source"));

        // Second import with updated coordinates
        let result = repo.import_timetable(&snapshot(
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
        let snap = snapshot(vec![station("A")], vec![], vec![], "source");
        repo.import_timetable(&snap);
        let result = repo.import_timetable(&snap);
        assert!(result.station_changes.is_empty());
    }

    // ---- diff_stations: additional cases ----

    #[test]
    fn station_name_change_is_reported_as_updated() {
        let mut repo = make_repo();
        repo.import_timetable(&snapshot(vec![station("A")], vec![], vec![], "source"));

        let renamed = ImportedStation::new(
            ImportedStationId::from("A".to_owned()),
            "Renamed".to_owned(),
            1.0,
            2.0,
        );
        let result = repo.import_timetable(&snapshot(vec![renamed], vec![], vec![], "source"));
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
        repo.import_timetable(&snapshot(
            vec![station("A"), station("B")],
            vec![],
            vec![],
            "source",
        ));

        // Second import only mentions A — B should still be in the DB.
        repo.import_timetable(&snapshot(vec![station("A")], vec![], vec![], "source"));
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
        repo.import_timetable(&snapshot(
            vec![station("A"), station("B")],
            vec![schedule("S1", &["20260101"])],
            vec![trip("R1", "A", "B", 100, 200)],
            "source",
        ));

        // Second import is empty for timetable data.
        repo.import_timetable(&snapshot(vec![], vec![], vec![], "source"));

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
        let result = repo.import_timetable(&snapshot(vec![], vec![], vec![], "source"));
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
        repo.import_timetable(&snapshot(
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
        let snap = Snapshot {
            stations: vec![],
            schedules: vec![s1, s2],
            trips: vec![],
            schedules_by_route: sbr,
            source: "source".to_owned(),
        };
        repo.import_timetable(&snap);
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
        repo.import_timetable(&snapshot(
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
}
