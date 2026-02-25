use derive_more::{Constructor, From};

pub mod importer;
pub mod parser;

pub trait ParseGTFS {
    fn stations(&self) -> &[GTFSStation];
    fn trips(&self) -> &[GTFSTrip];
}

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

impl GTFSStation {
    pub fn id(&self) -> &GTFSStationId {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn lat(&self) -> f64 {
        self.lat
    }
    pub fn lon(&self) -> f64 {
        self.lon
    }
    pub fn stops(&self) -> &[GTFSStopId] {
        &self.stops
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct GTFSTrip {
    origin: GTFSStopId,
    destination: GTFSStopId,
    departure: usize,
    arrival: usize,
}

impl GTFSTrip {
    pub fn origin(&self) -> &GTFSStopId {
        &self.origin
    }
    pub fn destination(&self) -> &GTFSStopId {
        &self.destination
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}

impl GTFSStationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
