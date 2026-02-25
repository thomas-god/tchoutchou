use derive_more::{Constructor, From};

pub mod gtfs;
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct ImportedStation {
    id: ImportedStationId,
    name: String,
    lat: f64,
    lon: f64,
}

impl ImportedStation {
    pub fn id(&self) -> &ImportedStationId {
        &self.id
    }
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTrip {
    origin: ImportedStationId,
    destination: ImportedStationId,
    departure: usize,
    arrival: usize,
}

impl ImportedTrip {
    pub fn origin(&self) -> &ImportedStationId {
        &self.origin
    }
    pub fn destination(&self) -> &ImportedStationId {
        &self.destination
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}
