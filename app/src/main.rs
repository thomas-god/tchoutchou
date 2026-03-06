use std::path::PathBuf;

use app::{
    app::schedule::ScheduleService,
    infra::{
        config::Config, cron::CronService, graph_cache::InMemoryGraphCache, http::HttpServer,
        repository::sqlite::SqliteRepository,
    },
};
use chrono::Utc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_env()?;

    let data_location = PathBuf::from(&config.data_location);
    let repo = SqliteRepository::open(
        data_location
            .join("train_data.db")
            .to_str()
            .expect("data_location is not valid UTF-8"),
    )?;
    let schedule_service = ScheduleService::new(repo, InMemoryGraphCache::default());
    let _ = schedule_service
        .graph(&format!("{}", Utc::now().format("%Y%m%d")))
        .expect("unable to warm graph cache");

    let cron =
        CronService::builder(data_location.join("cron-state.txt")).build(schedule_service.clone());
    let http_server = HttpServer::new(config, schedule_service).await?;

    tokio::select! {
        result = http_server.run() => { result?; }
        _ = cron.run() => {}
    }

    Ok(())
}
