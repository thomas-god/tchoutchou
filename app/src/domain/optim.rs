use std::collections::HashMap;

use derive_more::{Constructor, From};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, From)]
pub struct StationId(usize);

#[derive(Debug, Clone, Constructor)]
pub struct Station {
    id: StationId,
    name: String,
}

impl PartialEq for Station {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
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
    nodes: HashMap<StationId, Station>,
    trips_by_nodes: HashMap<StationId, Vec<Trip>>,
}

#[derive(Debug, Clone, Constructor, PartialEq)]
pub struct Destination {
    station: StationId,
    trips: Vec<Trip>,
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
            for trip in trips.iter().filter(|trip| {
                destination.trips.iter().all(|station| {
                    station.origin != trip.destination && station.destination != trip.destination
                })
            }) {
                let mut new_trips = destination.trips.clone();
                new_trips.push(trip.clone());
                new_destinations.push(Destination::new(trip.destination, new_trips));
            }
        }
    }

    destinations.extend(new_destinations);
    destinations
}

#[cfg(test)]
mod test_find_destinations {
    use super::*;

    fn graph_with_one_trip() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
        ]);
        let trips_by_nodes = HashMap::from_iter(vec![(
            StationId(1),
            vec![Trip::new(StationId(1), StationId(2), 100, 200)],
        )]);

        Graph::new(nodes, trips_by_nodes)
    }

    fn graph_with_two_trips_same_origin() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
            (
                StationId::from(3),
                Station::new(StationId(3), "Marseille".into()),
            ),
        ]);
        let trips_by_nodes = HashMap::from_iter(vec![(
            StationId(1),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 200),
                Trip::new(StationId(1), StationId(3), 100, 500),
            ],
        )]);

        Graph::new(nodes, trips_by_nodes)
    }

    fn graph_with_two_trips_with_loopback() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
        ]);
        let trips_by_nodes = HashMap::from_iter(vec![(
            StationId(1),
            vec![
                Trip::new(StationId(1), StationId(2), 100, 200),
                Trip::new(StationId(1), StationId(1), 100, 500),
            ],
        )]);

        Graph::new(nodes, trips_by_nodes)
    }

    fn graph_with_one_connection() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
        ]);
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

        Graph::new(nodes, trips_by_nodes)
    }

    fn graph_with_one_connection_and_loopbacks() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
        ]);
        let trips_by_nodes = HashMap::from_iter(vec![
            (
                StationId(1),
                vec![Trip::new(StationId(1), StationId(2), 100, 200)],
            ),
            (
                StationId(2),
                vec![
                    // New, unvisited station
                    Trip::new(StationId(2), StationId(3), 300, 500),
                    // Loopback to origin station
                    Trip::new(StationId(2), StationId(1), 300, 500),
                    // Loopback to current station
                    Trip::new(StationId(2), StationId(2), 300, 500),
                ],
            ),
        ]);

        Graph::new(nodes, trips_by_nodes)
    }

    fn graph_with_one_connection_and_one_direct() -> Graph {
        let nodes = HashMap::from_iter(vec![
            (
                StationId::from(1),
                Station::new(StationId(1), "Paris".into()),
            ),
            (
                StationId::from(2),
                Station::new(StationId(2), "Lyon".into()),
            ),
        ]);
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

        Graph::new(nodes, trips_by_nodes)
    }

    #[test]
    fn test_find_destinations_no_trip_for_origin() {
        let origin = StationId::from(2);
        let graph = graph_with_one_trip();

        let destinations = find_destinations(&origin, &graph);

        assert!(destinations.is_empty());
    }

    #[test]
    fn test_find_destinations_one_trip() {
        let origin = StationId::from(1);
        let graph = graph_with_one_trip();

        let destinations = find_destinations(&origin, &graph);

        assert_eq!(destinations.len(), 1);
        assert_eq!(
            destinations.first().unwrap(),
            &Destination::new(
                StationId(2),
                vec![Trip::new(StationId(1), StationId(2), 100, 200)]
            )
        )
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
    fn test_find_destinations_dont_loopback() {
        let origin = StationId::from(1);
        let graph = graph_with_two_trips_with_loopback();

        let destinations = find_destinations(&origin, &graph);

        assert_eq!(destinations.len(), 1);
        assert_eq!(
            destinations,
            vec![Destination::new(
                StationId(2),
                vec![Trip::new(StationId(1), StationId(2), 100, 200)]
            ),]
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
    fn test_find_destinations_with_one_connection_and_dont_loopback() {
        let origin = StationId::from(1);
        let graph = graph_with_one_connection_and_loopbacks();

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
