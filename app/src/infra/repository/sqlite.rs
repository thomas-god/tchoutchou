use std::collections::HashMap;

use rusqlite::{Connection, Result, params};

use crate::app::schedule::{
    ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
    ImportedTripLeg, TrainDataRepository,
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

impl TrainDataRepository for SqliteRepository {
    fn save_stations(&mut self, stations: &[ImportedStation]) {
        println!("Persisting {} stations", stations.len());
        let tx = self
            .conn
            .transaction()
            .expect("save_stations: begin transaction failed");
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO stations (id, name, lat, lon)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .expect("save_stations: prepare failed");
            for s in stations {
                stmt.execute(params![s.id().as_str(), s.name(), s.lat(), s.lon()])
                    .expect("save_stations: INSERT failed");
            }
        }
        tx.commit().expect("save_stations: commit failed");
    }

    fn save_schedules(&mut self, schedules: &[ImportedSchedule]) {
        println!("Persisting {} schedules", schedules.len());
        let tx = self
            .conn
            .transaction()
            .expect("save_schedules: begin transaction failed");
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO schedules (id, dates)
                     VALUES (?1, ?2)",
                )
                .expect("save_schedules: prepare failed");
            for s in schedules {
                let dates = s.dates().join(",");
                stmt.execute(params![s.id().as_str(), dates])
                    .expect("save_schedules: INSERT failed");
            }
        }
        tx.commit().expect("save_schedules: commit failed");
    }

    fn save_trips(&mut self, trips: &[ImportedTripLeg]) {
        println!("Persisting {} trips", trips.len());
        let tx = self
            .conn
            .transaction()
            .expect("save_trips: begin transaction failed");
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO trips (route, origin, destination, departure, arrival)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .expect("save_trips: prepare failed");
            for t in trips {
                stmt.execute(params![
                    t.route().as_str(),
                    t.origin().as_str(),
                    t.destination().as_str(),
                    t.departure() as i64,
                    t.arrival() as i64,
                ])
                .expect("save_trips: INSERT failed");
            }
        }
        tx.commit().expect("save_trips: commit failed");
    }

    fn save_schedules_by_route(
        &mut self,
        mapping: &HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
    ) {
        let tx = self
            .conn
            .transaction()
            .expect("save_schedules_by_route: begin transaction failed");
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR IGNORE INTO route_schedules (route_id, schedule_id)
                     VALUES (?1, ?2)",
                )
                .expect("save_schedules_by_route: prepare failed");
            for (route, schedules) in mapping {
                for schedule in schedules {
                    stmt.execute(params![route.as_str(), schedule.as_str()])
                        .expect("save_schedules_by_route: INSERT failed");
                }
            }
        }
        tx.commit().expect("save_schedules_by_route: commit failed");
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

    #[test]
    fn round_trip_stations() {
        let mut repo = make_repo();
        let input = vec![station("A"), station("B")];
        repo.save_stations(&input);
        let mut result = repo.all_stations();
        result.sort_by_key(|s| s.id().as_str().to_owned());
        assert_eq!(result, input);
    }

    #[test]
    fn round_trip_schedules() {
        let mut repo = make_repo();
        let input = vec![schedule("S1", &["20260101", "20260102"])];
        repo.save_schedules(&input);
        assert_eq!(repo.all_schedules(), input);
    }

    #[test]
    fn round_trip_trips() {
        let mut repo = make_repo();
        let input = vec![trip("R1", "A", "B", 100, 200)];
        repo.save_trips(&input);
        assert_eq!(repo.all_trips(), input);
    }

    #[test]
    fn round_trip_schedules_by_route() {
        let mut repo = make_repo();
        let mut mapping: HashMap<ImportedRouteId, Vec<ImportedScheduleId>> = HashMap::new();
        mapping
            .entry(ImportedRouteId::from("R1".to_owned()))
            .or_default()
            .push(ImportedScheduleId::from("S1".to_owned()));
        repo.save_schedules_by_route(&mapping);
        assert_eq!(repo.schedules_by_route(), mapping);
    }

    #[test]
    fn save_is_idempotent_for_stations() {
        let mut repo = make_repo();
        let s = vec![station("A")];
        repo.save_stations(&s);
        repo.save_stations(&s); // INSERT OR REPLACE — should not error
        assert_eq!(repo.all_stations().len(), 1);
    }
}
