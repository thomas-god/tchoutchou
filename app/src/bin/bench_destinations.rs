use std::{fs::OpenOptions, io::Write, time::Instant};

use app::{
    app::schedule::ScheduleService,
    domain::optim::CityId,
    infra::{
        caches::{InMemoryDestinationsCache, InMemoryGraphCache},
        repository::{geospatial::NominatimGeospatialRepository, sqlite::SqliteRepository},
    },
};
use clap::Parser;
use rusqlite::Connection;

/// Benchmark find_destinations execution time for a given origin station.
/// Results can be appended to a JSONL file to track optimisation progress over time.
#[derive(Parser)]
struct Cli {
    /// Path to the SQLite database file
    #[arg(long, default_value = "data/train_data.db")]
    db: String,

    /// Timetable date (YYYYMMDD)
    #[arg(long, default_value = "20260502")]
    date: String,

    /// Data source for the origin station (e.g. fr, db, es)
    #[arg(long)]
    source: String,

    /// Name of the origin station (case-insensitive substring match against source station names)
    #[arg(long)]
    station: String,

    /// File to append results to (JSONL format)
    #[arg(long, default_value = "bench_results.jsonl")]
    output: String,

    /// Number of times to run find_destinations (results are averaged to reduce noise)
    #[arg(long, default_value_t = 5)]
    runs: usize,

    /// Label for this run (e.g. "baseline", "remove Y")
    #[arg(long)]
    description: Option<String>,
}

/// Resolve a (source, station name) pair to an (internal_id, canonical_name) by querying
/// the DB directly. Returns all matches so the caller can handle ambiguity.
fn resolve_station(
    conn: &Connection,
    source: &str,
    name: &str,
) -> anyhow::Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "
        SELECT DISTINCT sm.internal_id, ist.name
        FROM stations s
        JOIN station_mappings sm ON sm.source = s.source AND sm.source_id = s.id
        JOIN internal_stations ist ON ist.id = sm.internal_id
        WHERE s.source = ?1
          AND LOWER(s.name) LIKE '%' || LOWER(?2) || '%'
        ORDER BY ist.name
        ",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![source, name], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Resolve origin station before building the full service.
    let conn = Connection::open(&cli.db)?;
    let candidates = resolve_station(&conn, &cli.source, &cli.station)?;
    drop(conn);

    let (internal_id, station_name) = match candidates.as_slice() {
        [] => anyhow::bail!(
            "no station found matching source='{}' name='{}'",
            cli.source,
            cli.station
        ),
        [single] => single.clone(),
        multiple => {
            let lines: Vec<String> = multiple
                .iter()
                .map(|(id, name)| format!("  {} — {}", id, name))
                .collect();
            anyhow::bail!(
                "{} stations match source='{}' name='{}', be more specific:\n{}",
                multiple.len(),
                cli.source,
                cli.station,
                lines.join("\n")
            );
        }
    };

    let origin_id = CityId::from(internal_id);
    println!(
        "Origin: {} (id={}, source={})",
        station_name, internal_id, cli.source
    );

    let repo = SqliteRepository::open(&cli.db)?;
    let schedule_service = ScheduleService::new(
        repo,
        InMemoryGraphCache::default(),
        InMemoryDestinationsCache::default(),
        NominatimGeospatialRepository::new("", "").expect("failed to load geospatial repository"),
    );

    let t0 = Instant::now();
    let _graph = schedule_service.warm(&cli.date);
    let graph_build_ms = t0.elapsed().as_millis();
    println!("Graph built in {}ms", graph_build_ms);

    let t1 = Instant::now();
    let (trips, _cities) = schedule_service
        .find_destinations(&cli.date, &origin_id)
        .expect("failed to compute trips");
    let first_ms = t1.elapsed().as_millis();

    let mut samples_ms: Vec<u128> = vec![first_ms];
    for _ in 1..cli.runs {
        let t = Instant::now();
        let _ = schedule_service.find_destinations(&cli.date, &origin_id);
        samples_ms.push(t.elapsed().as_millis());
    }

    samples_ms.sort_unstable();
    let min_ms = *samples_ms.first().unwrap();
    let max_ms = *samples_ms.last().unwrap();
    let mean_ms = samples_ms.iter().sum::<u128>() / samples_ms.len() as u128;
    let median_ms = samples_ms[samples_ms.len() / 2];

    println!(
        "origin='{}' (id={}) date={} destinations={} graph_build={}ms find_destinations: min={}ms median={}ms mean={}ms (over {} runs){}",
        station_name,
        internal_id,
        cli.date,
        trips.len(),
        graph_build_ms,
        min_ms,
        median_ms,
        mean_ms,
        cli.runs,
        cli.description
            .as_deref()
            .map(|d| format!(" ({})", d))
            .unwrap_or_default(),
    );

    let timestamp = chrono::Utc::now().to_rfc3339();
    let record = serde_json::json!({
        "timestamp": timestamp,
        "description": cli.description,
        "source": cli.source,
        "station_name": station_name,
        "station_id": internal_id,
        "date": cli.date,
        "destination_count": trips.len(),
        "graph_build_ms": graph_build_ms,
        "runs": cli.runs,
        "find_destinations_ms": {
            "min": min_ms,
            "median": median_ms,
            "mean": mean_ms,
            "max": max_ms,
            "samples": samples_ms,
        },
    });
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.output)?;
    writeln!(file, "{}", record)?;
    println!("Result appended to {}", cli.output);

    Ok(())
}
