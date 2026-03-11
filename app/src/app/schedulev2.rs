use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use derive_more::{Constructor, From};

use crate::{
    app::{
        ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
        ImportedTripLeg, TrainDataToImport, schedule::GraphCache,
    },
    domain::optim::{City, CityId, Graph, TripLeg},
};

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd)]
pub struct CityInformation {
    name: String,
    country: String,
    lat: f64,
    lon: f64,
}

impl CityInformation {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn country(&self) -> &str {
        &self.country
    }

    pub fn lat(&self) -> f64 {
        self.lat
    }

    pub fn lon(&self) -> f64 {
        self.lon
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct ScheduleDataToImport {
    train_data: TrainDataToImport,
    station_to_city: HashMap<ImportedStationId, CityInformation>,
}

impl ScheduleDataToImport {
    pub fn stations(&self) -> &[ImportedStation] {
        &self.train_data.stations
    }
    pub fn trip_legs(&self) -> &[ImportedTripLeg] {
        &self.train_data.legs
    }
    pub fn schedules(&self) -> &[ImportedSchedule] {
        &self.train_data.schedules
    }
    pub fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
        &self.train_data.schedules_by_route
    }
    pub fn source(&self) -> &str {
        &self.train_data.source
    }
    pub fn station_to_city(&self) -> &HashMap<ImportedStationId, CityInformation> {
        &self.station_to_city
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// `ScheduleService` related types.
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct InternalStationId(i64);

impl InternalStationId {
    pub fn value(&self) -> &i64 {
        &self.0
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct InternalTripLeg {
    origin: InternalStationId,
    destination: InternalStationId,
    departure: usize,
    arrival: usize,
}

impl InternalTripLeg {
    pub fn origin(&self) -> &InternalStationId {
        &self.origin
    }
    pub fn destination(&self) -> &InternalStationId {
        &self.destination
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleDataImportResult {}

/// Persistence contract for stations, trips and schedules.
pub trait ScheduleDataRepository {
    /// Atomically replace all timetable data (trips, schedules, route–schedule mappings)
    /// and upsert stations, returning information about which stations are new or changed.
    /// For each incoming source station that has no existing mapping to an internal
    /// station, a new [`InternalStation`] is created and linked automatically.
    fn import_timetable(&mut self, data: ScheduleDataToImport) -> ScheduleDataImportResult;

    /// Return only the trips whose route runs on `date` (format `YYYYMMDD`).
    fn legs_for_date(&self, date: &str) -> Vec<InternalTripLeg>;

    /// Return all source-to-internal station mappings.
    fn stations_to_city(&self) -> HashMap<InternalStationId, CityId>;

    /// Return up to `limit` internal stations whose name contains `query`
    /// (case-insensitive), ordered alphabetically. Intended for autocomplete.
    fn search_cities_by_name(&self, query: &str, limit: usize) -> Vec<City>;
}

pub trait GeospatialRepository: Clone + Send + Sync + 'static {
    fn match_stations_to_cities(
        &self,
        stations: &[ImportedStation],
    ) -> impl Future<Output = HashMap<ImportedStationId, CityInformation>> + Send;
}

/// Application service that aggregates data from various importers, persists it through a
/// [`TrainDataRepository`], and exposes a [`Graph`] ready for the optimisation algorithms in
/// [`crate::domain::optim`].
pub struct ScheduleService<R: ScheduleDataRepository, GC: GraphCache, GR: GeospatialRepository> {
    repository: Arc<Mutex<R>>,
    geo: Arc<GR>,
    graph_cache: Arc<GC>,
}

impl<R: ScheduleDataRepository, GC: GraphCache, GR: GeospatialRepository> Clone
    for ScheduleService<R, GC, GR>
{
    fn clone(&self) -> Self {
        Self {
            repository: self.repository.clone(),
            geo: self.geo.clone(),
            graph_cache: self.graph_cache.clone(),
        }
    }
}

impl<R: ScheduleDataRepository, GC: GraphCache, GR: GeospatialRepository>
    ScheduleService<R, GC, GR>
{
    pub fn new(repository: R, cache: GC, geo: GR) -> Self {
        Self {
            repository: Arc::new(Mutex::new(repository)),
            graph_cache: Arc::new(cache),
            geo: Arc::new(geo),
        }
    }

    pub async fn ingest(
        &mut self,
        data: TrainDataToImport,
    ) -> Result<ScheduleDataImportResult, ()> {
        let city_mapping = self.geo.match_stations_to_cities(data.stations()).await;

        let result = self.repository.lock().map_err(|_| ()).map(|mut repo| {
            repo.import_timetable(ScheduleDataToImport {
                train_data: data,
                station_to_city: city_mapping,
            })
        })?;
        self.graph_cache.clear();
        Ok(result)
    }

    /// Return up to `limit` [`InternalStation`]s whose name contains `query` (case-insensitive),
    /// ordered alphabetically. Intended for autocomplete.
    pub fn search_cities_by_name(&self, query: &str, limit: usize) -> Result<Vec<City>, ()> {
        self.repository
            .lock()
            .map_err(|_| ())
            .map(|repo| repo.search_cities_by_name(query, limit))
    }

    /// Build a [`Graph`] from trips active on `date` (format `YYYYMMDD`).
    ///
    /// [`ImportedStation`]s are resolved to their canonical [`InternalStationId`] so that
    /// connections to the same physical station are shared across providers.
    ///
    /// The result is an [`Arc`] into the internal cache. Repeated calls for the same date return
    /// a new handle to the same allocation — no graph data is ever copied.
    pub fn graph(&self, date: &str) -> Result<Arc<Graph>, ()> {
        // Fast path: return a cached handle without touching the repository.
        if let Some(graph) = self.graph_cache.get(date) {
            return Ok(graph);
        }

        // Cache miss: load from the repository, then populate the cache.
        let start = Instant::now();
        let graph = {
            let repo = self.repository.lock().map_err(|_| ())?;
            let legs = repo.legs_for_date(date);
            let mappings = repo.stations_to_city();
            tracing::info!(
                duration = format!("{:?}", start.elapsed()),
                date,
                "Graph loaded"
            );
            Arc::new(build_graph(&legs, &mappings))
        };

        self.graph_cache.insert(date, Arc::clone(&graph));

        Ok(graph)
    }
}

/// Map imported data to domain types.
///
/// [`ImportedStation`]s are resolved through `mappings` to their canonical [`InternalStationId`],
/// so that two providers whose stations share the same internal station are connected in the
/// resulting graph. Each [`StationId`] directly mirrors the corresponding [`InternalStationId`]
/// value.
fn build_graph(legs: &[InternalTripLeg], mappings: &HashMap<InternalStationId, CityId>) -> Graph {
    let mut legs_by_city: HashMap<CityId, Vec<TripLeg>> = HashMap::new();

    for leg in legs {
        let Some(&origin) = mappings.get(leg.origin()) else {
            continue;
        };
        let Some(&destination) = mappings.get(leg.destination()) else {
            continue;
        };
        legs_by_city.entry(origin).or_default().push(TripLeg::new(
            origin,
            destination,
            leg.departure(),
            leg.arrival(),
        ));
    }

    Graph::new(legs_by_city)
}

#[cfg(test)]
pub mod test_utils {
    use mockall::mock;

    use crate::infra::graph_cache::InMemoryGraphCache;

    use super::*;

    mock! {
        pub ScheduleDataRepository {}

        impl Clone for ScheduleDataRepository {
            fn clone(&self) -> Self;
        }

        impl ScheduleDataRepository for ScheduleDataRepository {
            fn import_timetable(&mut self, data: ScheduleDataToImport) -> ScheduleDataImportResult;
            fn legs_for_date(&self, date: &str) -> Vec<InternalTripLeg>;
            fn stations_to_city(&self) -> HashMap<InternalStationId, CityId>;
            fn search_cities_by_name(&self, query: &str, limit: usize) -> Vec<City>;
        }
    }

    mock! {
        pub GeospatialRepository {}

        impl Clone for GeospatialRepository {
            fn clone(&self) -> Self;
        }

        impl GeospatialRepository for GeospatialRepository {
             async fn match_stations_to_cities(
                &self,
                stations: &[ImportedStation],
            ) -> HashMap<ImportedStationId, CityInformation>;
        }
    }

    pub fn make_service(
        repo: MockScheduleDataRepository,
        geo: MockGeospatialRepository,
    ) -> ScheduleService<MockScheduleDataRepository, InMemoryGraphCache, MockGeospatialRepository>
    {
        ScheduleService::new(
            repo,
            crate::infra::graph_cache::InMemoryGraphCache::default(),
            geo,
        )
    }
}

#[cfg(test)]
mod tests {

    use crate::app::schedulev2::test_utils::{
        MockGeospatialRepository, MockScheduleDataRepository, make_service,
    };

    use super::*;

    const TEST_DATE: &str = "20260303";

    // -- helpers --

    fn station(id: &str) -> ImportedStation {
        ImportedStation::new(
            ImportedStationId::from(id.to_owned()),
            id.to_owned(),
            0.0,
            0.0,
        )
    }

    fn siid(id: i64) -> InternalStationId {
        InternalStationId::from(id)
    }

    fn cid(id: i64) -> CityId {
        CityId::from(id)
    }

    fn empty_result() -> ScheduleDataImportResult {
        ScheduleDataImportResult {}
    }

    fn make_importer(source: &str, station_ids: &[&str]) -> TrainDataToImport {
        TrainDataToImport::new(
            station_ids.iter().map(|id| station(id)).collect(),
            vec![],
            vec![],
            HashMap::new(),
            source.to_owned(),
        )
    }

    // ---- graph building ----

    #[test]
    fn ingest_builds_graph() {
        // trips_for_date returns already-filtered trips
        let trips = vec![InternalTripLeg::new(siid(12), siid(142), 100, 200)];
        let mappings = HashMap::from([(siid(12), cid(1)), (siid(142), cid(2))]);

        let mut mock = MockScheduleDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_stations_to_city().return_const(mappings);

        let mut geo = MockGeospatialRepository::new();
        geo.expect_match_stations_to_cities()
            .once()
            .returning(|_| HashMap::new());

        let mut service = make_service(mock, geo);
        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.legs_from(CityId::from(1)).len(), 1);
    }

    #[test]
    fn empty_repository_produces_empty_graph() {
        let mut mock = MockScheduleDataRepository::new();
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(vec![]);
        mock.expect_stations_to_city().return_const(HashMap::new());
        let geo = MockGeospatialRepository::new();

        let service = make_service(mock, geo);
        let graph = service.graph(TEST_DATE).expect("should build graph");
        assert_eq!(graph.legs_from(CityId::from(0)).len(), 0);
    }

    // ---- graph cache ----

    #[test]
    fn graph_hits_repository_only_once_for_same_date() {
        let trips = vec![InternalTripLeg::new(siid(12), siid(142), 100, 200)];
        let mappings = HashMap::from([(siid(12), cid(1)), (siid(142), cid(2))]);

        let mut mock = MockScheduleDataRepository::new();
        // trips_for_date and station_mappings must each be called exactly once despite
        // two graph() calls for the same date.
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .times(1)
            .return_const(trips);
        mock.expect_stations_to_city()
            .times(1)
            .return_const(mappings);
        let geo = MockGeospatialRepository::new();

        let service = make_service(mock, geo);
        let g1 = service.graph(TEST_DATE).expect("first call");
        let g2 = service.graph(TEST_DATE).expect("second call (cached)");
        assert_eq!(
            g1.legs_from(CityId::from(1)).len(),
            g2.legs_from(CityId::from(1)).len()
        );
    }

    #[test]
    fn ingest_invalidates_graph_cache() {
        let trips_before = vec![InternalTripLeg::new(siid(12), siid(142), 100, 200)];
        let trips_after = vec![];
        let mappings = HashMap::from([(siid(12), cid(1)), (siid(142), cid(2))]);

        let mut mock = MockScheduleDataRepository::new();
        mock.expect_import_timetable()
            .times(1)
            .returning(|_| empty_result());
        // trips_for_date is called twice: once before ingest (cache miss) and once
        // after ingest (cache invalidated, so another miss).
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .times(2)
            .returning({
                let mut call = 0usize;
                move |_| {
                    call += 1;
                    if call == 1 {
                        trips_before.clone()
                    } else {
                        trips_after.clone()
                    }
                }
            });
        mock.expect_stations_to_city()
            .times(2)
            .return_const(mappings);
        let mut geo = MockGeospatialRepository::new();
        geo.expect_match_stations_to_cities()
            .once()
            .returning(|_| HashMap::new());

        let mut service = make_service(mock, geo);

        let g_before = service.graph(TEST_DATE).expect("before ingest");
        assert_eq!(g_before.legs_from(CityId::from(1)).len(), 1);

        let _ = service.ingest(make_importer("source", &["A", "B"]));

        let g_after = service.graph(TEST_DATE).expect("after ingest");
        assert_eq!(g_after.legs_from(CityId::from(1)).len(), 0);
    }
}
