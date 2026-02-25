use derive_more::{Constructor, From};

pub mod importer;
pub mod parser;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct GTFSStationId(String);

/// A station can contain several, possibly abstract, stops. For example `GTFSStationId` "Paris Gare
/// de Lyon" can contain `GTFSStopId`s "Paris Gare de Lyon - TGV" and "Paris Gare de Lyon - OUIGO"
/// amongst others.
#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSStopId(String);

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct GTFSStation {
    id: GTFSStationId,
    name: String,
    lat: f64,
    lon: f64,
    stops: Vec<GTFSStopId>,
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct GTFSTrip {
    origin: GTFSStopId,
    destination: GTFSStopId,
    departure: usize,
    arrival: usize,
}
