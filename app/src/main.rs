use std::path::PathBuf;

use app::{
    app::{schedule::ScheduleService, schedulev2::ScheduleService as ServiceV2},
    infra::{
        config::{Config, load_env},
        cron::CronService,
        graph_cache::InMemoryGraphCache,
        http::{HttpServer, v2::HttpServerv2},
        importers::gtfs::{
            GTFSRouteType, fetcher::GTFSFetcher, importer::GTFSImporter, parsers::GTFSParser,
        },
        repository::{
            geospatial::NominatimGeospatialRepository, sqlite::SqliteRepository,
            sqlitev2::SqliteRepository as RepositoryV2,
        },
    },
};
use chrono::Utc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    if let Ok(mode) = load_env("MODE")
        && &mode == "v2"
    {
        tracing::info!("Running V2 app");
        _main_v2().await
    } else {
        tracing::info!("Running V1 app");
        _main().await
    }
}

async fn _main() -> anyhow::Result<()> {
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

async fn _main_v2() -> anyhow::Result<()> {
    let config = Config::from_env()?;

    let data_location = PathBuf::from(&config.data_location);
    let repo = RepositoryV2::open(
        data_location
            .join("train_data-v2.db")
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
    let mut schedule_service = ServiceV2::new(repo, InMemoryGraphCache::default(), geospatial);
    let _ = schedule_service
        .graph(&format!("{}", Utc::now().format("%Y%m%d")))
        .expect("unable to warm graph cache");

    let archive =
        GTFSFetcher::fetch("https://ssl.renfe.com/gtransit/Fichero_AV_LD/google_transit.zip")
            .await
            .map_err(|e| anyhow::anyhow!("fetch renfe: {e}"))?;

    let parser = GTFSParser::parse(archive.path().to_str().unwrap())
        .map_err(|e| anyhow::anyhow!("parse renfe: {e}"))?;

    let importer = GTFSImporter::from_parser(&parser, "renfe", &[GTFSRouteType::Rail]);

    schedule_service
        .ingest(importer.as_data())
        .await
        .map_err(|_| anyhow::anyhow!("ingest renfe failed"))?;

    let http_server = HttpServerv2::new(config, schedule_service).await?;

    tokio::select! {
        result = http_server.run() => { result?; }
    }

    Ok(())
}
