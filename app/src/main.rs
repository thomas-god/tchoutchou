use std::path::PathBuf;

use app::{
    app::schedulev2::ScheduleService,
    infra::{
        config::Config,
        cron::CronService,
        graph_cache::InMemoryGraphCache,
        http::v2::HttpServerv2,
        repository::{
            geospatial::NominatimGeospatialRepository, sqlitev2::SqliteRepository as RepositoryV2,
        },
    },
};
use chrono::Utc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_env()?;

    let data_location = PathBuf::from(&config.data_location);
    let repo = RepositoryV2::open(
        data_location
            .join("train_data_v2.db")
            .to_str()
            .expect("data_location is not valid UTF-8"),
    )?;
    let geospatial = NominatimGeospatialRepository::new(
        "http://localhost:8080",
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
    let http_server = HttpServerv2::new(config, schedule_service).await?;

    tokio::select! {
        result = http_server.run() => { result?; }
        _ = cron.run() => {}
    }

    Ok(())
}
