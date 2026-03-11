use anyhow::Context;
use axum::{
    Router,
    http::{
        HeaderValue, Method,
        header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
    },
    routing::get,
};
use tokio::net;
use tower_http::cors::CorsLayer;

pub mod handlers;

use crate::{
    app::schedule::ScheduleService,
    infra::{
        config::Config,
        graph_cache::InMemoryGraphCache,
        http::handlers::{autocomplete_city, get_destinations},
        repository::{geospatial::NominatimGeospatialRepository, sqlite::SqliteRepository},
    },
};

#[derive(Clone)]
pub struct AppState {
    schedule: ScheduleService<SqliteRepository, InMemoryGraphCache, NominatimGeospatialRepository>,
}

pub struct HttpServer {
    router: axum::Router,
    listener: net::TcpListener,
}

impl HttpServer {
    pub async fn new(
        config: Config,
        schedule_service: ScheduleService<
            SqliteRepository,
            InMemoryGraphCache,
            NominatimGeospatialRepository,
        >,
    ) -> anyhow::Result<Self> {
        let trace_layer = tower_http::trace::TraceLayer::new_for_http().make_span_with(
            |request: &axum::extract::Request<_>| {
                let uri = request.uri().to_string();
                tracing::info_span!("http_request", method = ?request.method(), uri)
            },
        );

        let state = AppState {
            schedule: schedule_service,
        };

        let origin = config
            .allow_origin
            .parse::<HeaderValue>()
            .with_context(|| format!("Not a valid origin {}", config.allow_origin))?;

        let mut router = axum::Router::new().nest("/api", routes());

        router = router.layer(trace_layer).layer(
            CorsLayer::new()
                .allow_headers([CONTENT_TYPE, COOKIE, SET_COOKIE])
                .allow_origin([origin])
                .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::PATCH])
                .allow_credentials(true),
        );

        let router = router.with_state(state);

        let listener = net::TcpListener::bind(format!("0.0.0.0:{}", config.server_port))
            .await
            .with_context(|| format!("failed to listen on {}", config.server_port))?;

        Ok(Self { router, listener })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::debug!("listening on {}", self.listener.local_addr().unwrap());
        axum::serve(self.listener, self.router)
            .await
            .context("received error from running server")?;
        Ok(())
    }
}

fn routes() -> Router<AppState> {
    Router::new()
        .route("/stations/autocomplete", get(autocomplete_city))
        .route("/destinations", get(get_destinations))
}
