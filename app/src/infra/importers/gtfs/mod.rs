use derive_more::{Constructor, From};

pub mod importer;
pub mod parser;

pub trait ParseGTFS {
    fn stations(&self) -> &[GTFSStation];
    fn trips(&self) -> &[GTFSTripLeg];
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

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSServiceId(String);

/// A `GTFSSchedule` represents a set of dates for which a train will run.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSService {
    id: GTFSServiceId,
    dates: Vec<String>,
}

/// A `GTFSRouteId` represent a set of stops that belong to the same physical train and trip. It can
/// be used amongst other things to find the `GTFSSchedule`s for a given `GTFSTrip`.
#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSRouteId(String);

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSTripId(String);

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct GTFSTripLeg {
    route: GTFSRouteId,
    origin: GTFSStopId,
    destination: GTFSStopId,
    departure: usize,
    arrival: usize,
}

impl GTFSTripLeg {
    pub fn route(&self) -> &GTFSRouteId {
        &self.route
    }
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

impl GTFSRouteId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
