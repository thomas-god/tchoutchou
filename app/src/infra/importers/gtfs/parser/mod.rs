use derive_more::{Constructor, From};

mod stations;
mod trips;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

/// A station can contain several, possibly abstract, stops. For example `StationId` "Paris Gare de
/// Lyon" can contain `StopId`s "Paris Gare de Lyon - TGV" and "Paris Gare de Lyon - OUIGO" amongst
/// others.
#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct ImportedStopId(String);

#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct ImportedStation {
    id: ImportedStationId,
    name: String,
    lat: f64,
    lon: f64,
    stops: Vec<ImportedStopId>,
}

#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTrip {
    origin: ImportedStopId,
    destination: ImportedStopId,
    departure: usize,
    arrival: usize,
}
