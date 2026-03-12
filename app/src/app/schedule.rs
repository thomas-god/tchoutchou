use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use derive_more::{Constructor, From};

use crate::{
    app::{
        ImportedRouteId, ImportedSchedule, ImportedScheduleId, ImportedStation, ImportedStationId,
        ImportedTripLeg, TrainDataToImport,
    },
    domain::optim::{City, CityId, DestinationFilters, Graph, Trip, TripLeg, find_trips},
};

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd)]
pub struct CityInformation {
    name: String,
    country: String,
    municipality: Option<String>,
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

    pub fn municipality(&self) -> &Option<String> {
        &self.municipality
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

    /// Return City objects for the given city IDs.
    fn cities_by_ids(&self, ids: &[CityId]) -> Vec<City>;
}

/// A thread-safe cache mapping date strings (`YYYYMMDD`) to pre-built [`Graph`]s.
///
/// Implementations are responsible for their own interior mutability so that methods can be
/// called with a shared `&self` reference. All clones must share the same underlying storage.
pub trait GraphCache: Send + Sync {
    fn get(&self, date: &str) -> Option<Arc<Graph>>;
    fn insert(&self, date: &str, graph: Arc<Graph>);
    fn clear(&self);
}

#[derive(Debug, Clone)]
pub struct GeospatialMappingResult {
    pub mapping: HashMap<ImportedStationId, CityInformation>,
    pub failures: Vec<GeospatialMappingFailure>,
}

#[derive(Debug, Clone)]
pub struct GeospatialMappingFailure {
    pub station_id: ImportedStationId,
    pub station_name: String,
    pub lat: f64,
    pub lon: f64,
    pub reason: FailureReason,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FailureReason {
    HttpError { status_code: u16 },
    MissingCityData,
    InvalidCoordinates,
    NetworkError,
}

pub trait GeospatialRepository: Clone + Send + Sync + 'static {
    fn match_stations_to_cities(
        &self,
        stations: &[ImportedStation],
    ) -> impl Future<Output = GeospatialMappingResult> + Send;
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
        let geo_result = self.geo.match_stations_to_cities(data.stations()).await;

        if !geo_result.failures.is_empty() {
            tracing::warn!(
                "Geospatial mapping had {} failures out of {} stations",
                geo_result.failures.len(),
                data.stations().len()
            );
        }

        let result = self.repository.lock().map_err(|_| ()).map(|mut repo| {
            repo.import_timetable(ScheduleDataToImport {
                train_data: data,
                station_to_city: geo_result.mapping,
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

    pub fn find_destinations(
        &self,
        date: &str,
        origin: &CityId,
        filters: &DestinationFilters,
    ) -> Result<(Vec<Trip>, Vec<City>), ()> {
        let graph = self.graph(date)?;

        let destinations = find_trips(origin, &graph, filters);

        let mut city_ids: Vec<CityId> = vec![];
        for trip in &destinations {
            city_ids.extend_from_slice(trip.visited_city_ids());
        }
        city_ids.sort();
        city_ids.dedup();

        // Fetch city information
        let cities = self
            .repository
            .lock()
            .map_err(|_| ())
            .map(|repo| repo.cities_by_ids(&city_ids))?;

        Ok((destinations, cities))
    }

    /// Build a [`Graph`] from trips active on `date` (format `YYYYMMDD`).
    ///
    /// [`ImportedStation`]s are resolved to their canonical [`InternalStationId`] so that
    /// connections to the same physical station are shared across providers.
    ///
    /// The result is an [`Arc`] into the internal cache. Repeated calls for the same date return
    /// a new handle to the same allocation — no graph data is ever copied.
    fn graph(&self, date: &str) -> Result<Arc<Graph>, ()> {
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

    pub fn warm(&self, date: &str) {
        let _ = self.graph(date);
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
            fn cities_by_ids(&self, ids: &[CityId]) -> Vec<City>;
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
            ) -> GeospatialMappingResult;
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

    use crate::app::schedule::test_utils::{
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

    #[tokio::test]
    async fn ingest_builds_graph() {
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
            .returning(|_| GeospatialMappingResult {
                mapping: HashMap::new(),
                failures: vec![],
            });

        let mut service = make_service(mock, geo);
        let _ = service.ingest(make_importer("source", &["A", "B"])).await;

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

    #[tokio::test]
    async fn ingest_invalidates_graph_cache() {
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
            .returning(|_| GeospatialMappingResult {
                mapping: HashMap::new(),
                failures: vec![],
            });

        let mut service = make_service(mock, geo);

        let g_before = service.graph(TEST_DATE).expect("before ingest");
        assert_eq!(g_before.legs_from(CityId::from(1)).len(), 1);

        let _ = service.ingest(make_importer("source", &["A", "B"])).await;

        let g_after = service.graph(TEST_DATE).expect("after ingest");
        assert_eq!(g_after.legs_from(CityId::from(1)).len(), 0);
    }

    // ---- find_destinations ----

    #[test]
    fn find_destinations_returns_trips_and_cities() {
        let trips = vec![
            InternalTripLeg::new(siid(1), siid(2), 100, 200),
            InternalTripLeg::new(siid(2), siid(3), 1200, 1300),
        ];
        let mappings = HashMap::from([(siid(1), cid(1)), (siid(2), cid(2)), (siid(3), cid(3))]);
        let paris = City::new(
            cid(1),
            "Paris".to_string(),
            "France".to_string(),
            48.8566,
            2.3522,
        );
        let london = City::new(
            cid(2),
            "London".to_string(),
            "UK".to_string(),
            51.5074,
            -0.1278,
        );
        let berlin = City::new(
            cid(3),
            "Berlin".to_string(),
            "Germany".to_string(),
            52.5200,
            13.4050,
        );

        let mut mock = MockScheduleDataRepository::new();
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_stations_to_city().return_const(mappings);
        mock.expect_cities_by_ids().times(1).returning(move |ids| {
            // Verify that the requested IDs include origin and all visited cities
            assert!(ids.contains(&cid(1))); // origin
            assert!(ids.contains(&cid(2))); // direct destination
            vec![paris.clone(), london.clone(), berlin.clone()]
        });

        let geo = MockGeospatialRepository::new();
        let service = make_service(mock, geo);

        let (trips, cities) = service
            .find_destinations(TEST_DATE, &cid(1), &DestinationFilters::default())
            .expect("find_destinations should succeed");

        assert_eq!(trips.len(), 2); // Direct trip to city 2, and one-connection trip to city 3
        assert_eq!(cities.len(), 3); // Paris, London, Berlin
        assert!(cities.iter().any(|c| c.name() == "Paris"));
        assert!(cities.iter().any(|c| c.name() == "London"));
        assert!(cities.iter().any(|c| c.name() == "Berlin"));
    }

    #[test]
    fn find_destinations_includes_origin_city() {
        let trips = vec![InternalTripLeg::new(siid(1), siid(2), 100, 200)];
        let mappings = HashMap::from([(siid(1), cid(1)), (siid(2), cid(2))]);
        let paris = City::new(
            cid(1),
            "Paris".to_string(),
            "France".to_string(),
            48.8566,
            2.3522,
        );
        let london = City::new(
            cid(2),
            "London".to_string(),
            "UK".to_string(),
            51.5074,
            -0.1278,
        );

        let mut mock = MockScheduleDataRepository::new();
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_stations_to_city().return_const(mappings);
        mock.expect_cities_by_ids().times(1).returning(move |ids| {
            // Origin must be in the requested IDs
            assert!(ids.contains(&cid(1)));
            vec![paris.clone(), london.clone()]
        });

        let geo = MockGeospatialRepository::new();
        let service = make_service(mock, geo);

        let (_, cities) = service
            .find_destinations(TEST_DATE, &cid(1), &DestinationFilters::default())
            .expect("find_destinations should succeed");

        assert!(cities.iter().any(|c| c.name() == "Paris")); // Origin city included
    }

    #[test]
    fn find_destinations_deduplicates_city_ids() {
        // Two trips to the same destination should not duplicate cities
        let trips = vec![
            InternalTripLeg::new(siid(1), siid(2), 100, 200),
            InternalTripLeg::new(siid(1), siid(2), 300, 400),
        ];
        let mappings = HashMap::from([(siid(1), cid(1)), (siid(2), cid(2))]);

        let mut mock = MockScheduleDataRepository::new();
        mock.expect_legs_for_date()
            .withf(|d| d == TEST_DATE)
            .return_const(trips);
        mock.expect_stations_to_city().return_const(mappings);
        mock.expect_cities_by_ids().times(1).returning(|ids| {
            // Should only request each unique city ID once
            let mut unique_ids = ids.to_vec();
            unique_ids.sort();
            unique_ids.dedup();
            assert_eq!(
                ids.len(),
                unique_ids.len(),
                "City IDs should be deduplicated"
            );
            vec![]
        });

        let geo = MockGeospatialRepository::new();
        let service = make_service(mock, geo);

        let _ = service.find_destinations(TEST_DATE, &cid(1), &DestinationFilters::default());
    }
}
