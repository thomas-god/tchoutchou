use std::path::PathBuf;

use app::{
    app::schedule::ScheduleService,
    infra::{
        config::Config,
        cron::CronService,
        graph_cache::InMemoryGraphCache,
        http::HttpServer,
        repository::{geospatial::NominatimGeospatialRepository, sqlite::SqliteRepository},
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
            .join("train_data_v2.db")
            .to_str()
            .expect("data_location is not valid UTF-8"),
    )?;
    let geospatial = NominatimGeospatialRepository::new(
        &config.nominatim_url,
        data_location
            .join("geo-cache.db")
            .to_str()
            .expect("geo-cache is not valid UTF-8"),
    )
    .expect("unable to build geospatial repository");
    let schedule_service = ScheduleService::new(repo, InMemoryGraphCache::default(), geospatial);
    schedule_service.warm(&format!("{}", Utc::now().format("%Y%m%d")));

    let cron =
        CronService::builder(data_location.join("cron-state.txt")).build(schedule_service.clone());
    let http_server = HttpServer::new(config, schedule_service).await?;

    tokio::select! {
        result = http_server.run() => { result?; }
        _ = cron.run() => {}
    }

    Ok(())
}
