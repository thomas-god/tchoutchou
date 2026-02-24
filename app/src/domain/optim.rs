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

#[derive(Debug, Clone, PartialEq)]
pub struct Destination {
    station: StationId,
    trips: Vec<Trip>,
    current_time: usize,
}

impl Destination {
    fn new(station: StationId, trips: Vec<Trip>) -> Self {
        Self {
            station,
            current_time: trips
                .iter()
                .map(|trip| trip.arrival)
                .max()
                .unwrap_or_default(),
            trips,
        }
    }

    /// Try to connect a `Trip` to itself if compatible (same origin, compatible departure,
    /// no loopback). Returns `None` if the trip is not compatible, otherwise `Some(Self)` with
    /// the new extended destination.
    fn try_connect_trip(&self, trip: &Trip) -> Option<Self> {
        if self.station != trip.origin {
            return None;
        }

        if self.trips.iter().any(|visitied| {
            visitied.origin == trip.destination || visitied.destination == trip.destination
        }) {
            return None;
        }

        if trip.departure <= self.current_time {
            return None;
        }

        let mut new_trips = self.trips.clone();
        new_trips.push(trip.clone());

        Some(Self {
            station: trip.destination,
            current_time: trip.arrival,
            trips: new_trips,
        })
    }

    fn find_connections_from(&self, trips: &[Trip]) -> Vec<Self> {
        trips
            .iter()
            .filter_map(|trip| self.try_connect_trip(trip))
            .collect()
    }
}

pub fn find_destinations(origin: &StationId, graph: &Graph) -> Vec<Destination> {
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

    let mut new_destinations = vec![];
    for destination in destinations.iter() {
        if let Some(trips) = graph.trips_by_nodes.get(&destination.station) {
            new_destinations.extend(destination.find_connections_from(trips));
        }
    }

    destinations.extend(new_destinations);
    destinations
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
                Trip::new(StationId(1), StationId(3), 100, 500),
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
                vec![Trip::new(StationId(2), StationId(3), 300, 500)],
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
                vec![Trip::new(StationId(2), StationId(3), 300, 600)],
            ),
        ]);

        Graph::new(trips_by_nodes)
    }

    #[test]
    fn test_find_destinations_no_trip_for_origin() {
        let origin = StationId::from(2);
        let graph = graph_with_one_trip();

        let destinations = find_destinations(&origin, &graph);

        assert!(destinations.is_empty());
    }

    #[test]
    fn test_find_destinations_two_trips() {
        let origin = StationId::from(1);
        let graph = graph_with_two_trips_same_origin();

        let destinations = find_destinations(&origin, &graph);

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
                    vec![Trip::new(StationId(1), StationId(3), 100, 500)]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_with_one_connection() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection();

        let destinations = find_destinations(&origin, &graph);

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
                        Trip::new(StationId(2), StationId(3), 300, 500)
                    ]
                )
            ]
        )
    }

    #[test]
    fn test_find_destinations_same_destination_direct_and_with_connection() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection_and_one_direct();

        let destinations = find_destinations(&origin, &graph);

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
                        Trip::new(StationId(2), StationId(3), 300, 600)
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
        let trip = Trip::new(StationId(3), StationId(4), 400, 500);

        assert!(destination.try_connect_trip(&trip).is_none())
    }

    #[test]
    fn test_try_connect_trip_to_destination_same_origin() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(4), 400, 500);

        assert_eq!(
            destination.try_connect_trip(&trip).unwrap(),
            Destination::new(
                StationId(4),
                vec![
                    Trip::new(StationId(1), StationId(2), 100, 300),
                    Trip::new(StationId(2), StationId(4), 400, 500)
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
        let trip = Trip::new(StationId(2), StationId(1), 400, 500);

        assert!(destination.try_connect_trip(&trip).is_none())
    }

    #[test]
    fn test_try_connect_trip_to_destination_already_visited() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(2), 400, 500);

        assert!(destination.try_connect_trip(&trip).is_none())
    }

    #[test]
    fn test_try_connect_trip_to_destination_incompatible_departure() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trip = Trip::new(StationId(2), StationId(3), 300, 500);

        assert!(destination.try_connect_trip(&trip).is_none())
    }

    #[test]
    fn test_match_new_destinations() {
        let destination = Destination::new(
            StationId(2),
            vec![Trip::new(StationId(1), StationId(2), 100, 300)],
        );
        let trips = vec![
            Trip::new(StationId(2), StationId(3), 400, 500),
            Trip::new(StationId(2), StationId(4), 400, 500),
            Trip::new(StationId(2), StationId(1), 400, 500),
        ];

        let new_destinations = destination.find_connections_from(&trips);

        assert_eq!(
            new_destinations,
            vec![
                Destination::new(
                    StationId(3),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 300),
                        Trip::new(StationId(2), StationId(3), 400, 500)
                    ],
                ),
                Destination::new(
                    StationId(4),
                    vec![
                        Trip::new(StationId(1), StationId(2), 100, 300),
                        Trip::new(StationId(2), StationId(4), 400, 500),
                    ],
                )
            ]
        )
    }
}
