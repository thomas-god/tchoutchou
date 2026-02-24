use std::collections::HashMap;

use derive_more::{Constructor, From};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, From)]
pub struct StationId(usize);

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

#[derive(Debug, Clone, Constructor)]
pub struct DestinationFilters {
    max_connections: usize,
    min_connection_duration: usize,
    max_duration: usize,
}

impl Default for DestinationFilters {
    fn default() -> Self {
        Self {
            max_connections: 2,
            min_connection_duration: 900,
            max_duration: 3600 * 12,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Destination {
    station: StationId,
    trips: Vec<Trip>,
    current_time: usize,
    duration: usize,
}

impl Destination {
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
        Self {
            station,
            current_time: arrival,
            duration: arrival - departure,
            trips: trips.clone(),
        }
    }

    /// Try to connect a `Trip` to itself if compatible (same origin, compatible departure,
    /// no loopback). Returns `None` if the trip is not compatible, otherwise `Some(Self)` with
    /// the new extended destination.
    fn try_connect_trip(&self, trip: &Trip, filters: &DestinationFilters) -> Option<Self> {
        if self.trips.len() > filters.max_connections {
            return None;
        }

        if self.station != trip.origin {
            return None;
        }

        if self.trips.iter().any(|visitied| {
            visitied.origin == trip.destination || visitied.destination == trip.destination
        }) {
            return None;
        }

        if trip.departure <= self.current_time + filters.min_connection_duration {
            return None;
        }

        if self.duration + trip.arrival - self.current_time > filters.max_duration {
            return None;
        }

        let mut new_trips = self.trips.clone();
        new_trips.push(trip.clone());

        Some(Self {
            station: trip.destination,
            current_time: trip.arrival,
            duration: self.duration + trip.arrival - self.current_time,
            trips: new_trips,
        })
    }

    fn find_connections_from(&self, trips: &[Trip], filters: &DestinationFilters) -> Vec<Self> {
        trips
            .iter()
            .filter_map(|trip| self.try_connect_trip(trip, filters))
            .collect()
    }
}

pub fn find_destinations(
    origin: &StationId,
    graph: &Graph,
    filters: &DestinationFilters,
) -> Vec<Destination> {
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

    find_new_destinations(graph, destinations, filters)
}

fn find_new_destinations(
    graph: &Graph,
    mut destinations: Vec<Destination>,
    filters: &DestinationFilters,
) -> Vec<Destination> {
    let mut new_destinations = vec![];

    for destination in destinations.iter() {
        if let Some(trips) = graph.trips_by_nodes.get(&destination.station) {
            new_destinations.extend(destination.find_connections_from(trips, filters));
        }
    }

    if new_destinations.is_empty() {
        return destinations;
    } else {
        destinations.extend(find_new_destinations(graph, new_destinations, filters));
        return destinations;
    }
}

#[cfg(test)]
mod test_find_destinations {
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
    fn test_find_destinations_same_destination_direct_and_with_connection() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection_and_one_direct();

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        assert_eq!(destinations.len(), 3);
        assert_eq!(
            destinations,
            vec![
                Destination::new(
                    StationId(2),
                    vec![Trip::new(StationId(1), StationId(2), 100, 200)]
                ),
                Destination::new(
                    StationId(3),
                    vec![Trip::new(StationId(1), StationId(3), 100, 500),]
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

        let destinations = find_destinations(&origin, &graph, &DestinationFilters::default());

        // assert_eq!(destinations.len(), 3);
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
}

#[cfg(test)]
mod test_destination_struct {
    use super::*;

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

        let new_destinations =
            destination.find_connections_from(&trips, &DestinationFilters::default());

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
