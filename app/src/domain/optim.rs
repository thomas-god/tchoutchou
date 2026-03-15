use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::Entry},
    time::Instant,
};

use derive_more::{Constructor, From};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, From)]
pub struct CityId(i64);

impl CityId {
    pub fn as_i64(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct City {
    id: CityId,
    name: String,
    country: String,
    lat: f64,
    lon: f64,
}

impl City {
    pub fn id(&self) -> &CityId {
        &self.id
    }
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

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct TripLeg {
    origin: CityId,
    destination: CityId,
    departure: usize,
    arrival: usize,
}

#[derive(Debug, Clone, Constructor)]
pub struct Graph {
    legs_by_city: HashMap<CityId, Vec<TripLeg>>,
}

#[cfg(test)]
impl Graph {
    pub fn legs_from(&self, station: CityId) -> &[TripLeg] {
        self.legs_by_city
            .get(&station)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct DestinationFilters {
    max_connections: usize,
    min_connection_duration: usize,
    max_duration: usize,
}

impl DestinationFilters {
    pub(crate) const MAX_CONNECTIONS_MIN: usize = 0;
    pub(crate) const MAX_CONNECTIONS_MAX: usize = 2;
    pub(crate) const MIN_CONNECTION_DURATION_MIN: usize = 0;
    pub(crate) const MIN_CONNECTION_DURATION_MAX: usize = 3600 * 6;
    pub(crate) const MAX_DURATION_MIN: usize = 3600;
    pub(crate) const MAX_DURATION_MAX: usize = 3600 * 24;

    pub fn new(
        max_connections: usize,
        min_connection_duration: usize,
        max_duration: usize,
    ) -> Self {
        Self {
            max_connections: max_connections
                .clamp(Self::MAX_CONNECTIONS_MIN, Self::MAX_CONNECTIONS_MAX),
            min_connection_duration: min_connection_duration.clamp(
                Self::MIN_CONNECTION_DURATION_MIN,
                Self::MIN_CONNECTION_DURATION_MAX,
            ),
            max_duration: max_duration.clamp(Self::MAX_DURATION_MIN, Self::MAX_DURATION_MAX),
        }
    }
}

impl Default for DestinationFilters {
    fn default() -> Self {
        Self {
            max_connections: 2,
            min_connection_duration: 900,
            max_duration: 3600 * 24,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Trip {
    destination: CityId,
    visited_cities: Vec<CityId>,
    nb_legs: usize,
    current_time: usize,
    duration: usize,
}

impl Ord for Trip {
    fn cmp(&self, other: &Self) -> Ordering {
        self.destination
            .cmp(&other.destination)
            .then(self.duration.cmp(&other.duration))
    }
}

impl PartialOrd for Trip {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Trip {
    fn eq(&self, other: &Self) -> bool {
        self.destination == other.destination && self.duration == other.duration
    }
}

impl Eq for Trip {}

impl Trip {
    pub fn destination(&self) -> i64 {
        self.destination.as_i64()
    }

    pub fn duration(&self) -> usize {
        self.duration
    }

    pub fn number_of_connections(&self) -> usize {
        self.nb_legs - 1
    }

    pub fn intermediary_city_ids(&self) -> &[CityId] {
        let len = self.visited_cities.len();
        if len <= 2 {
            &[]
        } else {
            &self.visited_cities[1..len - 1]
        }
    }

    pub fn visited_city_ids(&self) -> &[CityId] {
        &self.visited_cities
    }

    fn new(destination: CityId, legs: Vec<TripLeg>) -> Self {
        let arrival = legs
            .iter()
            .map(|trip| trip.arrival)
            .max()
            .unwrap_or_default();
        let departure = legs
            .iter()
            .map(|trip| trip.departure)
            .min()
            .unwrap_or_default();
        let visited_stations = legs
            .first()
            .map(|t| t.origin)
            .into_iter()
            .chain(legs.iter().map(|t| t.destination))
            .collect();
        Self {
            destination,
            nb_legs: legs.len(),
            visited_cities: visited_stations,
            current_time: arrival,
            duration: arrival - departure,
        }
    }

    /// Try to connect a `TripLeg` to itself if compatible (same origin, compatible departure,
    /// no loopback). Returns `None` if the trip is not compatible, otherwise `Some(Self)` with
    /// the new extended destination.
    fn try_connect_leg(&self, trip: &TripLeg, filters: &DestinationFilters) -> Option<Self> {
        if self.nb_legs > filters.max_connections {
            return None;
        }

        if self.destination != trip.origin {
            return None;
        }

        if self.visited_cities.contains(&trip.destination) {
            return None;
        }

        if trip.departure <= self.current_time + filters.min_connection_duration {
            return None;
        }

        if self.duration + trip.arrival - self.current_time > filters.max_duration {
            return None;
        }

        let mut new_visited = self.visited_cities.clone();
        new_visited.push(trip.destination);

        Some(Self {
            destination: trip.destination,
            current_time: trip.arrival,
            duration: self.duration + trip.arrival - self.current_time,
            nb_legs: self.nb_legs + 1,
            visited_cities: new_visited,
        })
    }

    fn find_connections_from<'a>(
        &'a self,
        legs: &'a [TripLeg],
        filters: &'a DestinationFilters,
    ) -> impl Iterator<Item = Self> + 'a {
        legs.iter()
            .filter_map(move |leg| self.try_connect_leg(leg, filters))
    }
}

/// Find all trips whithin the `graph` starting at `origin` city and matching the `filters`.
pub fn find_trips(origin: &CityId, graph: &Graph, filters: &DestinationFilters) -> Vec<Trip> {
    let start = Instant::now();
    let mut trips = vec![];

    let Some(first_legs) = graph.legs_by_city.get(origin) else {
        return vec![];
    };

    for leg in first_legs.iter().filter(|leg| leg.destination != *origin) {
        trips.push(Trip::new(leg.destination, vec![leg.clone()]));
    }

    let res = dedup_trips_by_destination(find_new_destinations(graph, trips, filters, 0));

    tracing::info!(
        duration = format!("{:?}", start.elapsed()),
        count = res.len(),
        "Destinations computed"
    );
    res
}

/// Extend a list of `trips` with new compatible legs from `graph`, matching `filters`.
fn find_new_destinations(
    graph: &Graph,
    mut trips: Vec<Trip>,
    filters: &DestinationFilters,
    nb_of_connections: usize,
) -> Vec<Trip> {
    if nb_of_connections >= filters.max_connections {
        return trips;
    }

    // Use a HashMap to deduplicate on the fly: only keep the shortest path to each city.
    let mut new_destinations: HashMap<CityId, Trip> = HashMap::new();

    for trip in trips.iter() {
        if let Some(legs) = graph.legs_by_city.get(&trip.destination) {
            for candidate_trip in trip.find_connections_from(legs, filters) {
                match new_destinations.entry(candidate_trip.destination) {
                    Entry::Occupied(mut existing_destination) => {
                        if candidate_trip.duration < existing_destination.get().duration {
                            *existing_destination.get_mut() = candidate_trip;
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(candidate_trip);
                    }
                }
            }
        }
    }

    if new_destinations.is_empty() {
        trips
    } else {
        let new_trips_vec: Vec<Trip> = new_destinations.into_values().collect();
        trips.extend(find_new_destinations(
            graph,
            new_trips_vec,
            filters,
            nb_of_connections + 1,
        ));
        trips
    }
}

#[cfg(test)]
mod test_destination_filters {
    use super::*;

    #[test]
    fn test_new_accepts_values_within_bounds() {
        let f = DestinationFilters::new(1, 900, 3600 * 12);
        assert_eq!(f.max_connections, 1);
        assert_eq!(f.min_connection_duration, 900);
        assert_eq!(f.max_duration, 3600 * 12);
    }

    #[test]
    fn test_new_clamps_max_connections_above_max() {
        let f = DestinationFilters::new(usize::MAX, 900, 3600 * 12);
        assert_eq!(f.max_connections, DestinationFilters::MAX_CONNECTIONS_MAX);
    }

    #[test]
    fn test_new_clamps_min_connection_duration_above_max() {
        let f = DestinationFilters::new(1, usize::MAX, 3600 * 12);
        assert_eq!(
            f.min_connection_duration,
            DestinationFilters::MIN_CONNECTION_DURATION_MAX
        );
    }

    #[test]
    fn test_new_clamps_max_duration_below_min() {
        let f = DestinationFilters::new(1, 900, 0);
        assert_eq!(f.max_duration, DestinationFilters::MAX_DURATION_MIN);
    }

    #[test]
    fn test_new_clamps_max_duration_above_max() {
        let f = DestinationFilters::new(1, 900, usize::MAX);
        assert_eq!(f.max_duration, DestinationFilters::MAX_DURATION_MAX);
    }
}

/// Keep only one trip for a given destination, keeping the shorter one.
fn dedup_trips_by_destination(mut trips: Vec<Trip>) -> Vec<Trip> {
    trips.sort();
    trips.dedup_by(|a, b| a.destination.eq(&b.destination));

    trips
}

#[cfg(test)]
mod test_find_destinations {
    use pretty_assertions::assert_eq;

    use super::*;

    fn graph_with_one_trip() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![(
            CityId(1),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        )]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_two_trips_same_origin() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![(
            CityId(1),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 200),
                TripLeg::new(CityId(1), CityId(3), 1200, 1300),
            ],
        )]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_one_connection() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                CityId(1),
                vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
            ),
            (
                CityId(2),
                vec![TripLeg::new(CityId(2), CityId(3), 1200, 1300)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_one_connection_and_one_direct() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                CityId(1),
                vec![
                    TripLeg::new(CityId(1), CityId(2), 100, 200),
                    TripLeg::new(CityId(1), CityId(3), 100, 500),
                ],
            ),
            (
                CityId(2),
                vec![TripLeg::new(CityId(2), CityId(3), 1200, 1300)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_2_connections() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                CityId(1),
                vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
            ),
            (
                CityId(2),
                vec![TripLeg::new(CityId(2), CityId(3), 1200, 1300)],
            ),
            (
                CityId(3),
                vec![TripLeg::new(CityId(3), CityId(4), 2300, 2400)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    #[test]
    fn test_find_destinations_no_trip_for_origin() {
        let origin = CityId::from(2);
        let graph = graph_with_one_trip();

        let destinations = find_trips(&origin, &graph, &DestinationFilters::default());

        assert!(destinations.is_empty());
    }

    #[test]
    fn test_find_destinations_two_trips() {
        let origin = CityId::from(1);
        let graph = graph_with_two_trips_same_origin();

        let destinations = find_trips(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Trip::new(
                    CityId(2),
                    vec![TripLeg::new(CityId(1), CityId(2), 100, 200)]
                ),
                Trip::new(
                    CityId(3),
                    vec![TripLeg::new(CityId(1), CityId(3), 1200, 1300)]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_with_one_connection() {
        let origin = CityId::from(1);
        let graph = graph_with_one_connection();

        let destinations = find_trips(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Trip::new(
                    CityId(2),
                    vec![TripLeg::new(CityId(1), CityId(2), 100, 200)]
                ),
                Trip::new(
                    CityId(3),
                    vec![
                        TripLeg::new(CityId(1), CityId(2), 100, 200),
                        TripLeg::new(CityId(2), CityId(3), 1200, 1300)
                    ]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_multiple_connections() {
        let origin = CityId::from(1);
        let graph = graph_with_2_connections();
        let filters = DestinationFilters::new(2, 900, 12 * 3600);

        let destinations = find_trips(&origin, &graph, &filters);

        assert_eq!(
            destinations,
            vec![
                Trip::new(
                    CityId(2),
                    vec![TripLeg::new(CityId(1), CityId(2), 100, 200)]
                ),
                Trip::new(
                    CityId(3),
                    vec![
                        TripLeg::new(CityId(1), CityId(2), 100, 200),
                        TripLeg::new(CityId(2), CityId(3), 1200, 1300)
                    ]
                ),
                Trip::new(
                    CityId(4),
                    vec![
                        TripLeg::new(CityId(1), CityId(2), 100, 200),
                        TripLeg::new(CityId(2), CityId(3), 1200, 1300),
                        TripLeg::new(CityId(3), CityId(4), 2300, 2400)
                    ]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_remove_duplicate_destination() {
        let origin = CityId::from(1);
        let graph = graph_with_one_connection_and_one_direct();

        let destinations = find_trips(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Trip::new(
                    CityId(2),
                    vec![TripLeg::new(CityId(1), CityId(2), 100, 200)]
                ),
                Trip::new(
                    CityId(3),
                    // Keep the fastest trips
                    vec![TripLeg::new(CityId(1), CityId(3), 100, 500),]
                ),
            ]
        )
    }
}

#[cfg(test)]
mod test_destination_ord {
    use super::*;

    #[test]
    fn test_ord_different_stations() {
        let a = Trip::new(
            CityId(1),
            vec![TripLeg::new(CityId(0), CityId(1), 100, 200)],
        );
        let b = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(0), CityId(2), 100, 200)],
        );

        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn test_ord_same_station_different_duration() {
        let short = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        );
        let long = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 500)],
        );

        assert!(short < long);
        assert!(long > short);
    }

    #[test]
    fn test_ord_equal() {
        let a = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        );
        let b = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        );

        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn test_ord_station_takes_priority_over_duration() {
        // Station 1 with long duration vs station 2 with short duration → station wins
        let a = Trip::new(
            CityId(1),
            vec![TripLeg::new(CityId(0), CityId(1), 100, 500)],
        );
        let b = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(0), CityId(2), 100, 200)],
        );

        assert!(a < b);
    }
}

#[cfg(test)]
mod test_destination_struct {
    use super::*;

    #[test]
    fn test_intermediary_station_ids_no_connection() {
        // Single trip: visited = [origin, dest] → no intermediaries
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        );

        assert_eq!(destination.intermediary_city_ids(), &[]);
    }

    #[test]
    fn test_intermediary_station_ids_one_connection() {
        // Two trips: visited = [1, 2, 3] → intermediary is [2]
        let destination = Trip::new(
            CityId(3),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 200),
                TripLeg::new(CityId(2), CityId(3), 1200, 1300),
            ],
        );

        assert_eq!(destination.intermediary_city_ids(), &[CityId(2)]);
    }

    #[test]
    fn test_intermediary_station_ids_two_connections() {
        // Three trips: visited = [1, 2, 3, 4] → intermediaries are [2, 3]
        let destination = Trip::new(
            CityId(4),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 200),
                TripLeg::new(CityId(2), CityId(3), 1200, 1300),
                TripLeg::new(CityId(3), CityId(4), 2300, 2400),
            ],
        );

        assert_eq!(destination.intermediary_city_ids(), &[CityId(2), CityId(3)]);
    }

    #[test]
    fn test_visited_city_ids_no_connection() {
        // Single trip: visited = [origin, dest]
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 200)],
        );

        assert_eq!(destination.visited_city_ids(), &[CityId(1), CityId(2)]);
    }

    #[test]
    fn test_visited_city_ids_one_connection() {
        // Two trips: visited = [1, 2, 3]
        let destination = Trip::new(
            CityId(3),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 200),
                TripLeg::new(CityId(2), CityId(3), 1200, 1300),
            ],
        );

        assert_eq!(
            destination.visited_city_ids(),
            &[CityId(1), CityId(2), CityId(3)]
        );
    }

    #[test]
    fn test_visited_city_ids_two_connections() {
        // Three trips: visited = [1, 2, 3, 4]
        let destination = Trip::new(
            CityId(4),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 200),
                TripLeg::new(CityId(2), CityId(3), 1200, 1300),
                TripLeg::new(CityId(3), CityId(4), 2300, 2400),
            ],
        );

        assert_eq!(
            destination.visited_city_ids(),
            &[CityId(1), CityId(2), CityId(3), CityId(4)]
        );
    }

    #[test]
    fn test_try_connect_trip_to_destination_wrong_origin() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(3), CityId(4), 1201, 1300);

        assert!(
            destination
                .try_connect_leg(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_same_origin() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(2), CityId(4), 1201, 1300);

        assert_eq!(
            destination
                .try_connect_leg(&trip, &DestinationFilters::default())
                .unwrap(),
            Trip::new(
                CityId(4),
                vec![
                    TripLeg::new(CityId(1), CityId(2), 100, 300),
                    TripLeg::new(CityId(2), CityId(4), 1201, 1300)
                ],
            )
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_origin_already_visited() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(2), CityId(1), 1201, 1300);

        assert!(
            destination
                .try_connect_leg(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_already_visited() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(2), CityId(2), 1201, 1300);

        assert!(
            destination
                .try_connect_leg(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_incompatible_departure() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(2), CityId(3), 310, 500);

        assert!(
            destination
                .try_connect_leg(
                    &trip,
                    &DestinationFilters {
                        min_connection_duration: 10,
                        ..Default::default()
                    }
                )
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_max_connections_reached() {
        let destination = Trip::new(
            CityId(3),
            vec![
                TripLeg::new(CityId(1), CityId(2), 100, 300),
                TripLeg::new(CityId(2), CityId(3), 1300, 1400),
            ],
        );
        let trip = TripLeg::new(CityId(3), CityId(4), 2400, 2500);

        assert!(
            destination
                .try_connect_leg(
                    &trip,
                    &DestinationFilters {
                        max_connections: 1,
                        ..Default::default()
                    }
                )
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_max_duration_reached() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trip = TripLeg::new(CityId(2), CityId(3), 1300, 1400);

        assert!(
            destination
                .try_connect_leg(
                    &trip,
                    &DestinationFilters {
                        max_duration: (300 - 100) + (1300 - 300) + (1400 - 1300) - 1,
                        ..Default::default()
                    }
                )
                .is_none()
        )
    }

    #[test]
    fn test_match_new_destinations() {
        let destination = Trip::new(
            CityId(2),
            vec![TripLeg::new(CityId(1), CityId(2), 100, 300)],
        );
        let trips = vec![
            TripLeg::new(CityId(2), CityId(3), 1201, 1300),
            TripLeg::new(CityId(2), CityId(4), 1201, 1300),
            TripLeg::new(CityId(2), CityId(1), 1201, 1300),
        ];

        let new_destinations: Vec<Trip> = destination
            .find_connections_from(&trips, &DestinationFilters::default())
            .collect();

        assert_eq!(
            new_destinations,
            vec![
                Trip::new(
                    CityId(3),
                    vec![
                        TripLeg::new(CityId(1), CityId(2), 100, 300),
                        TripLeg::new(CityId(2), CityId(3), 1201, 1300)
                    ],
                ),
                Trip::new(
                    CityId(4),
                    vec![
                        TripLeg::new(CityId(1), CityId(2), 100, 300),
                        TripLeg::new(CityId(2), CityId(4), 1201, 1300),
                    ],
                )
            ]
        )
    }
}
