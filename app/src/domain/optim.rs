use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::Entry},
    time::Instant,
};

use derive_more::{Constructor, From};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, From)]
pub struct StationId(i64);

impl StationId {
    pub fn as_i64(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct Trip {
    origin: StationId,
    destination: StationId,
    departure: usize,
    arrival: usize,
}

#[derive(Debug, Clone, Constructor)]
pub struct Graph {
    trips_by_nodes: HashMap<StationId, Vec<Trip>>,
}

#[cfg(test)]
impl Graph {
    pub fn trips_from(&self, station: StationId) -> &[Trip] {
        self.trips_by_nodes
            .get(&station)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct DestinationFilters {
    max_connections: usize,
    min_connection_duration: usize,
    max_duration: usize,
}

impl Default for DestinationFilters {
    fn default() -> Self {
        Self {
            max_connections: 1,
            min_connection_duration: 900,
            max_duration: 3600 * 12,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Destination {
    station: StationId,
    visited_stations: Vec<StationId>,
    nb_trips: usize,
    current_time: usize,
    duration: usize,
}

impl Ord for Destination {
    fn cmp(&self, other: &Self) -> Ordering {
        self.station
            .cmp(&other.station)
            .then(self.duration.cmp(&other.duration))
    }
}

impl PartialOrd for Destination {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Destination {
    fn eq(&self, other: &Self) -> bool {
        self.station == other.station && self.duration == other.duration
    }
}

impl Eq for Destination {}

impl Destination {
    pub fn station_id(&self) -> i64 {
        self.station.as_i64()
    }

    pub fn duration(&self) -> usize {
        self.duration
    }

    pub fn connections_count(&self) -> usize {
        self.nb_trips - 1
    }

    pub fn intermediary_station_ids(&self) -> &[StationId] {
        let len = self.visited_stations.len();
        if len <= 2 {
            &[]
        } else {
            &self.visited_stations[1..len - 1]
        }
    }

    fn new(station: StationId, trips: Vec<Trip>) -> Self {
        let arrival = trips
            .iter()
            .map(|trip| trip.arrival)
            .max()
            .unwrap_or_default();
        let departure = trips
            .iter()
            .map(|trip| trip.departure)
            .min()
            .unwrap_or_default();
        let visited_stations = trips
            .first()
            .map(|t| t.origin)
            .into_iter()
            .chain(trips.iter().map(|t| t.destination))
            .collect();
        Self {
            station,
            nb_trips: trips.len(),
            visited_stations,
            current_time: arrival,
            duration: arrival - departure,
        }
    }

    /// Try to connect a `Trip` to itself if compatible (same origin, compatible departure,
    /// no loopback). Returns `None` if the trip is not compatible, otherwise `Some(Self)` with
    /// the new extended destination.
    fn try_connect_trip(&self, trip: &Trip, filters: &DestinationFilters) -> Option<Self> {
        if self.nb_trips > filters.max_connections {
            return None;
        }

        if self.station != trip.origin {
            return None;
        }

        if self.visited_stations.contains(&trip.destination) {
            return None;
        }

        if trip.departure <= self.current_time + filters.min_connection_duration {
            return None;
        }

        if self.duration + trip.arrival - self.current_time > filters.max_duration {
            return None;
        }

        let mut new_visited = self.visited_stations.clone();
        new_visited.push(trip.destination);

        Some(Self {
            station: trip.destination,
            current_time: trip.arrival,
            duration: self.duration + trip.arrival - self.current_time,
            nb_trips: self.nb_trips + 1,
            visited_stations: new_visited,
        })
    }

    fn find_connections_from<'a>(
        &'a self,
        trips: &'a [Trip],
        filters: &'a DestinationFilters,
    ) -> impl Iterator<Item = Self> + 'a {
        trips
            .iter()
            .filter_map(move |trip| self.try_connect_trip(trip, filters))
    }
}

pub fn find_destinations(
    origin: &StationId,
    graph: &Graph,
    filters: &DestinationFilters,
) -> Vec<Destination> {
    let start = Instant::now();
    let mut destinations = vec![];

    let Some(first_trips) = graph.trips_by_nodes.get(origin) else {
        return vec![];
    };

    for trip in first_trips
        .iter()
        .filter(|trip| trip.destination != *origin)
    {
        destinations.push(Destination::new(trip.destination, vec![trip.clone()]));
    }

    let res = remove_duplicate_destinations(find_new_destinations(graph, destinations, filters, 0));

    tracing::info!(
        duration = format!("{:?}", start.elapsed()),
        count = res.len(),
        "Destinations computed"
    );
    res
}

fn find_new_destinations(
    graph: &Graph,
    mut destinations: Vec<Destination>,
    filters: &DestinationFilters,
    nb_of_connections: usize,
) -> Vec<Destination> {
    if nb_of_connections >= filters.max_connections {
        return destinations;
    }

    // Use a HashMap to deduplicate on the fly: only keep the shortest path to each station.
    let mut new_destinations: HashMap<StationId, Destination> = HashMap::new();

    for destination in destinations.iter() {
        if let Some(trips) = graph.trips_by_nodes.get(&destination.station) {
            for candidate_destination in destination.find_connections_from(trips, filters) {
                match new_destinations.entry(candidate_destination.station) {
                    Entry::Occupied(mut existing_destination) => {
                        if candidate_destination.duration < existing_destination.get().duration {
                            *existing_destination.get_mut() = candidate_destination;
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(candidate_destination);
                    }
                }
            }
        }
    }

    if new_destinations.is_empty() {
        destinations
    } else {
        let new_dests_vec: Vec<Destination> = new_destinations.into_values().collect();
        destinations.extend(find_new_destinations(
            graph,
            new_dests_vec,
            filters,
            nb_of_connections + 1,
        ));
        destinations
    }
}

/// Keep trip with the shorter duration (see impl of `Ord` for `Destination`).
fn remove_duplicate_destinations(mut destinations: Vec<Destination>) -> Vec<Destination> {
    destinations.sort();
    destinations.dedup_by(|a, b| a.station.eq(&b.station));

    destinations
}

#[cfg(test)]
mod test_find_destinations {
    use pretty_assertions::assert_eq;

    use super::*;

    fn graph_with_one_trip() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![(
            StationId(1),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        )]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_two_trips_same_origin() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![(
            StationId(1),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 200),
                Trip::new(StationId(1), StationId(3), 1200, 1300),
            ],
        )]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_one_connection() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                StationId(1),
                vec![Trip::new(StationId(1), StationId(2), 100, 200)],
            ),
            (
                StationId(2),
                vec![Trip::new(StationId(2), StationId(3), 1200, 1300)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_one_connection_and_one_direct() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                StationId(1),
                vec![
                    Trip::new(StationId(1), StationId(2), 100, 200),
                    Trip::new(StationId(1), StationId(3), 100, 500),
                ],
            ),
            (
                StationId(2),
                vec![Trip::new(StationId(2), StationId(3), 1200, 1300)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    fn graph_with_2_connections() -> Graph {
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                StationId(1),
                vec![Trip::new(StationId(1), StationId(2), 100, 200)],
            ),
            (
                StationId(2),
                vec![Trip::new(StationId(2), StationId(3), 1200, 1300)],
            ),
            (
                StationId(3),
                vec![Trip::new(StationId(3), StationId(4), 2300, 2400)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    #[test]
    fn test_find_destinations_no_trip_for_origin() {
        let origin = StationId::from(2);
        let graph = graph_with_one_trip();

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        assert!(destinations.is_empty());
    }

    #[test]
    fn test_find_destinations_two_trips() {
        let origin = StationId::from(1);
        let graph = graph_with_two_trips_same_origin();

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Destination::new(
                    StationId(2),
                    vec![Trip::new(StationId(1), StationId(2), 100, 200)]
                ),
                Destination::new(
                    StationId(3),
                    vec![Trip::new(StationId(1), StationId(3), 1200, 1300)]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_with_one_connection() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection();

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Destination::new(
                    StationId(2),
                    vec![Trip::new(StationId(1), StationId(2), 100, 200)]
                ),
                Destination::new(
                    StationId(3),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 200),
                        Trip::new(StationId(2), StationId(3), 1200, 1300)
                    ]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_multiple_connections() {
        let origin = StationId::from(1);
        let graph = graph_with_2_connections();
        let filters = DestinationFilters::new(2, 900, 12 * 3600);

        let destinations = find_destinations(&origin, &graph, &filters);

        assert_eq!(
            destinations,
            vec![
                Destination::new(
                    StationId(2),
                    vec![Trip::new(StationId(1), StationId(2), 100, 200)]
                ),
                Destination::new(
                    StationId(3),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 200),
                        Trip::new(StationId(2), StationId(3), 1200, 1300)
                    ]
                ),
                Destination::new(
                    StationId(4),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 200),
                        Trip::new(StationId(2), StationId(3), 1200, 1300),
                        Trip::new(StationId(3), StationId(4), 2300, 2400)
                    ]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_remove_duplicate_destination() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection_and_one_direct();

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 2);
        assert_eq!(
            destinations,
            vec![
                Destination::new(
                    StationId(2),
                    vec![Trip::new(StationId(1), StationId(2), 100, 200)]
                ),
                Destination::new(
                    StationId(3),
                    // Keep the fastest trips
                    vec![Trip::new(StationId(1), StationId(3), 100, 500),]
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
        let a = Destination::new(
            StationId(1),
            vec![Trip::new(StationId(0), StationId(1), 100, 200)],
        );
        let b = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(0), StationId(2), 100, 200)],
        );

        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn test_ord_same_station_different_duration() {
        let short = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        );
        let long = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 500)],
        );

        assert!(short < long);
        assert!(long > short);
    }

    #[test]
    fn test_ord_equal() {
        let a = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        );
        let b = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        );

        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn test_ord_station_takes_priority_over_duration() {
        // Station 1 with long duration vs station 2 with short duration → station wins
        let a = Destination::new(
            StationId(1),
            vec![Trip::new(StationId(0), StationId(1), 100, 500)],
        );
        let b = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(0), StationId(2), 100, 200)],
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
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        );

        assert_eq!(destination.intermediary_station_ids(), &[]);
    }

    #[test]
    fn test_intermediary_station_ids_one_connection() {
        // Two trips: visited = [1, 2, 3] → intermediary is [2]
        let destination = Destination::new(
            StationId(3),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 200),
                Trip::new(StationId(2), StationId(3), 1200, 1300),
            ],
        );

        assert_eq!(destination.intermediary_station_ids(), &[StationId(2)]);
    }

    #[test]
    fn test_intermediary_station_ids_two_connections() {
        // Three trips: visited = [1, 2, 3, 4] → intermediaries are [2, 3]
        let destination = Destination::new(
            StationId(4),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 200),
                Trip::new(StationId(2), StationId(3), 1200, 1300),
                Trip::new(StationId(3), StationId(4), 2300, 2400),
            ],
        );

        assert_eq!(
            destination.intermediary_station_ids(),
            &[StationId(2), StationId(3)]
        );
    }

    #[test]
    fn test_try_connect_trip_to_destination_wrong_origin() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(3), StationId(4), 1201, 1300);

        assert!(
            destination
                .try_connect_trip(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_same_origin() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(4), 1201, 1300);

        assert_eq!(
            destination
                .try_connect_trip(&trip, &DestinationFilters::default())
                .unwrap(),
            Destination::new(
                StationId(4),
                vec![
                    Trip::new(StationId(1), StationId(2), 100, 300),
                    Trip::new(StationId(2), StationId(4), 1201, 1300)
                ],
            )
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_origin_already_visited() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(1), 1201, 1300);

        assert!(
            destination
                .try_connect_trip(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_already_visited() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(2), 1201, 1300);

        assert!(
            destination
                .try_connect_trip(&trip, &DestinationFilters::default())
                .is_none()
        )
    }

    #[test]
    fn test_try_connect_trip_to_destination_incompatible_departure() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(3), 310, 500);

        assert!(
            destination
                .try_connect_trip(
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
        let destination = Destination::new(
            StationId(3),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 300),
                Trip::new(StationId(2), StationId(3), 1300, 1400),
            ],
        );
        let trip = Trip::new(StationId(3), StationId(4), 2400, 2500);

        assert!(
            destination
                .try_connect_trip(
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
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(3), 1300, 1400);

        assert!(
            destination
                .try_connect_trip(
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
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trips = vec![
            Trip::new(StationId(2), StationId(3), 1201, 1300),
            Trip::new(StationId(2), StationId(4), 1201, 1300),
            Trip::new(StationId(2), StationId(1), 1201, 1300),
        ];

        let new_destinations: Vec<Destination> = destination
            .find_connections_from(&trips, &DestinationFilters::default())
            .collect();

        assert_eq!(
            new_destinations,
            vec![
                Destination::new(
                    StationId(3),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 300),
                        Trip::new(StationId(2), StationId(3), 1201, 1300)
                    ],
                ),
                Destination::new(
                    StationId(4),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 300),
                        Trip::new(StationId(2), StationId(4), 1201, 1300),
                    ],
                )
            ]
        )
    }
}
