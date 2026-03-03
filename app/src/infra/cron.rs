use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use tokio::time::{Instant, sleep_until};

use crate::{
    app::schedule::ScheduleService,
    infra::{
        importers::gtfs::{fetcher::GTFSFetcher, importer::GTFSImporter, parsers::GTFSParser},
        repository::sqlite::SqliteRepository,
    },
};

/// The time at which every job runs each day (UTC).
const DAILY_RUN_TIME: NaiveTime = match NaiveTime::from_hms_opt(2, 0, 0) {
    Some(t) => t,
    None => panic!("invalid DAILY_RUN_TIME"),
};

type Task = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

struct RegisteredJob {
    name: String,
    task: Task,
}

// ── Builder ────────────────────────────────────────────────────────────────────────────────────

pub struct CronServiceBuilder {
    state_path: PathBuf,
}

impl CronServiceBuilder {
    /// Consume the builder by wiring all predefined import jobs to `schedule_service` and
    /// returning a ready-to-run [`CronService`].
    pub fn build(self, schedule_service: ScheduleService<SqliteRepository>) -> CronService {
        let mut service = CronService {
            jobs: Vec::new(),
            state_path: self.state_path,
        };

        // SNCF import
        let svc = schedule_service.clone();
        service.register("sncf", move || {
            let mut svc = svc.clone();
            async move {
                let archive = GTFSFetcher::fetch(
                    "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
                )
                .await
                .map_err(|e| anyhow::anyhow!("fetch sncf: {e}"))?;

                let parser = GTFSParser::parse(archive.path().to_str().unwrap())
                    .map_err(|e| anyhow::anyhow!("parse sncf: {e}"))?;

                let importer = GTFSImporter::from_parser(&parser, "sncf");

                svc.ingest(importer.as_data())
                    .map_err(|_| anyhow::anyhow!("ingest sncf failed"))?;

                Ok(())
            }
        });

        // DB (Deutsche Bahn) import
        let svc = schedule_service.clone();
        service.register("db", move || {
            let mut svc = svc.clone();
            async move {
                let archive =
                    GTFSFetcher::fetch("https://download.gtfs.de/germany/fv_free/latest.zip")
                        .await
                        .map_err(|e| anyhow::anyhow!("fetch db: {e}"))?;

                let parser = GTFSParser::parse(archive.path().to_str().unwrap())
                    .map_err(|e| anyhow::anyhow!("parse db: {e}"))?;

                let importer = GTFSImporter::from_parser(&parser, "db");

                svc.ingest(importer.as_data())
                    .map_err(|_| anyhow::anyhow!("ingest db failed"))?;

                Ok(())
            }
        });

        // Renfe import
        let svc = schedule_service;
        service.register("renfe", move || {
            let mut svc = svc.clone();
            async move {
                let archive = GTFSFetcher::fetch(
                    "https://ssl.renfe.com/gtransit/Fichero_AV_LD/google_transit.zip",
                )
                .await
                .map_err(|e| anyhow::anyhow!("fetch renfe: {e}"))?;

                let parser = GTFSParser::parse(archive.path().to_str().unwrap())
                    .map_err(|e| anyhow::anyhow!("parse renfe: {e}"))?;

                let importer = GTFSImporter::from_parser(&parser, "renfe");

                svc.ingest(importer.as_data())
                    .map_err(|_| anyhow::anyhow!("ingest renfe failed"))?;

                Ok(())
            }
        });
        service
    }
}

// ── CronService ────────────────────────────────────────────────────────────────────────────────

/// A minimal daily scheduler.
///
/// All jobs run once per day at [`DAILY_RUN_TIME`] (02:00 UTC). On startup:
/// - If a job has never run, it executes immediately.
/// - If the job last ran before today's 02:00, it executes immediately (it's overdue).
/// - If the job already ran today (after today's 02:00), it sleeps until tomorrow's 02:00.
///
/// After each successful run the state file is updated so the next startup can skip a redundant
/// run. On failure the job retries after 60 seconds.
///
/// State file format (plain text, one entry per line):
/// ```text
/// sncf=2026-03-02T02:05:01Z
/// db=2026-03-02T02:08:42Z
/// ```
pub struct CronService {
    jobs: Vec<RegisteredJob>,
    state_path: PathBuf,
}

impl CronService {
    /// Create a builder.
    pub fn builder(state_path: impl Into<PathBuf>) -> CronServiceBuilder {
        CronServiceBuilder {
            state_path: state_path.into(),
        }
    }

    /// Start all jobs and run them indefinitely.
    ///
    /// Spawns one tokio task per job. Only returns if every task finishes, which should not
    /// happen in normal operation.
    pub async fn run(self) {
        let state = Arc::new(Mutex::new(load_state(&self.state_path)));
        let state_path = Arc::new(self.state_path);

        let handles: Vec<_> = self
            .jobs
            .into_iter()
            .map(|job| {
                let last_run = state.lock().unwrap().get(&job.name).copied();
                let state = Arc::clone(&state);
                let state_path = Arc::clone(&state_path);
                tokio::spawn(run_job(job, last_run, state, state_path))
            })
            .collect();

        for handle in handles {
            let _ = handle.await;
        }
    }

    fn register<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.jobs.push(RegisteredJob {
            name: name.into(),
            task: Arc::new(move || Box::pin(task())),
        });
    }
}

// ── Scheduling logic ───────────────────────────────────────────────────────────────────────────

async fn run_job(
    job: RegisteredJob,
    last_run: Option<DateTime<Utc>>,
    state: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    state_path: Arc<PathBuf>,
) {
    let delay = first_run_delay(last_run, Utc::now());

    if delay.is_zero() {
        tracing::info!(job = %job.name, "job is due immediately");
    } else {
        tracing::info!(
            job = %job.name,
            delay_secs = delay.as_secs(),
            "job scheduled, waiting until 02:00 UTC"
        );
        sleep_until(Instant::now() + delay).await;
    }

    loop {
        tracing::info!(job = %job.name, "running job");

        match (job.task)().await {
            Ok(()) => {
                tracing::info!(job = %job.name, "job succeeded");
                persist_run(&job.name, Utc::now(), &state, &state_path);
                sleep_until(next_daily_instant()).await;
            }
            Err(err) => {
                tracing::error!(job = %job.name, error = %err, "job failed, retrying in 60 s");
                sleep_until(Instant::now() + Duration::from_secs(60)).await;
            }
        }
    }
}

/// Compute how long to wait before the first execution.
///
/// - Never ran → zero (immediate).
/// - Last ran before today's 02:00 → zero (overdue).
/// - Last ran after today's 02:00 → delay until tomorrow's 02:00.
fn first_run_delay(last_run: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    let Some(last) = last_run else {
        return Duration::ZERO;
    };

    let today_at_2 = Utc.from_utc_datetime(&now.date_naive().and_time(DAILY_RUN_TIME));
    if last >= today_at_2 {
        // Already ran today – delay until tomorrow's 02:00.
        let tomorrow_at_2 = today_at_2 + chrono::Duration::days(1);
        (tomorrow_at_2 - now).to_std().unwrap_or(Duration::ZERO)
    } else {
        // Overdue – run immediately.
        Duration::ZERO
    }
}

/// The wall-clock moment of today's [`DAILY_RUN_TIME`] in UTC.
fn today_daily_run() -> DateTime<Utc> {
    let today = Utc::now().date_naive();
    Utc.from_utc_datetime(&today.and_time(DAILY_RUN_TIME))
}

/// The [`Instant`] corresponding to the next occurrence of [`DAILY_RUN_TIME`] (always tomorrow
/// from the perspective of this call, i.e. at least ~0 s and at most ~24 h away).
fn next_daily_instant() -> Instant {
    let next = today_daily_run() + chrono::Duration::days(1);
    let secs_until = (next - Utc::now()).num_seconds().max(0) as u64;
    Instant::now() + Duration::from_secs(secs_until)
}

// ── State file I/O ─────────────────────────────────────────────────────────────────────────────

fn load_state(path: &Path) -> HashMap<String, DateTime<Utc>> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };

    content
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            let ts = value.trim().parse::<DateTime<Utc>>().ok()?;
            Some((key.trim().to_owned(), ts))
        })
        .collect()
}

fn persist_run(
    job_name: &str,
    timestamp: DateTime<Utc>,
    state: &Mutex<HashMap<String, DateTime<Utc>>>,
    path: &Path,
) {
    let mut guard = state.lock().unwrap();
    guard.insert(job_name.to_owned(), timestamp);

    let content = guard
        .iter()
        .map(|(k, v)| format!("{}={}\n", k, v.to_rfc3339()))
        .collect::<String>();

    if let Err(err) = std::fs::write(path, content) {
        tracing::warn!(job = %job_name, error = %err, "failed to persist cron state");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn dt(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn empty_state() -> Arc<Mutex<HashMap<String, DateTime<Utc>>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    // ── State I/O ──────────────────────────────────────────────────────────────────────────────

    #[test]
    fn state_round_trips() {
        let file = NamedTempFile::new().unwrap();
        let state = empty_state();
        let ts = dt("2026-03-02T02:05:00Z");

        persist_run("sncf", ts, &state, file.path());

        let on_disk = load_state(file.path());
        assert_eq!(on_disk["sncf"], ts);
    }

    #[test]
    fn state_multiple_jobs_are_independent() {
        let file = NamedTempFile::new().unwrap();
        let state = empty_state();
        let ts_sncf = dt("2026-03-02T02:05:00Z");
        let ts_db = dt("2026-03-02T02:08:00Z");

        persist_run("sncf", ts_sncf, &state, file.path());
        persist_run("db", ts_db, &state, file.path());

        let on_disk = load_state(file.path());
        assert_eq!(on_disk["sncf"], ts_sncf);
        assert_eq!(on_disk["db"], ts_db);
    }

    #[test]
    fn load_state_returns_empty_for_missing_file() {
        let state = load_state(Path::new("/nonexistent/cron-state.txt"));
        assert!(state.is_empty());
    }

    #[test]
    fn load_state_ignores_malformed_lines() {
        let file = NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "sncf=2026-03-02T02:05:00Z\ngarbage\ndb=not-a-date\n",
        )
        .unwrap();

        let state = load_state(file.path());
        assert_eq!(state.len(), 1);
        assert!(state.contains_key("sncf"));
    }

    // ── Scheduling logic ───────────────────────────────────────────────────────────────────────

    #[test]
    fn never_ran_runs_immediately() {
        let now = dt("2026-03-03T10:00:00Z");
        assert_eq!(first_run_delay(None, now), Duration::ZERO);
    }

    #[test]
    fn last_ran_yesterday_is_overdue() {
        let now = dt("2026-03-03T10:00:00Z");
        let last = dt("2026-03-02T02:05:00Z");
        assert_eq!(first_run_delay(Some(last), now), Duration::ZERO);
    }

    #[test]
    fn last_ran_before_todays_window_is_overdue() {
        let now = dt("2026-03-03T10:00:00Z");
        let last = dt("2026-03-03T01:00:00Z"); // before 02:00 today
        assert_eq!(first_run_delay(Some(last), now), Duration::ZERO);
    }

    #[test]
    fn already_ran_today_waits_until_tomorrow() {
        let now = dt("2026-03-03T10:00:00Z");
        let last = dt("2026-03-03T02:05:00Z"); // after 02:00 today
        // next run: 2026-03-04T02:00:00Z → 16 hours away
        assert_eq!(
            first_run_delay(Some(last), now),
            Duration::from_secs(16 * 3600)
        );
    }
}
