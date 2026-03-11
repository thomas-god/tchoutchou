use std::collections::HashMap;

use derive_more::{Constructor, From};

pub mod schedule;

///////////////////////////////////////////////////////////////////////////////////////////////////
// `Imported*`` types describe the expected shapes of data to be ingested by the schedule service.
///////////////////////////////////////////////////////////////////////////////////////////////////

/// A station represents a physical place where trains can depart from and arrive at.
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
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn lat(&self) -> f64 {
        self.lat
    }
    pub fn lon(&self) -> f64 {
        self.lon
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedStationId(String);

impl ImportedStationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A trip leg represents a train leaving its *origin* station at *departure* time and reaching its
/// *destination* station at *arrival time*. For example, a train leaving Paris Gare de Lyon at
/// 09:20 and reaching Lyon Part-Dieu at 11:15.
///
/// The dates for which the train will actually run needs to be retrieved by the schedules
/// associated with the trips's [`ImportedRouteId`].
#[derive(Debug, Clone, Constructor, PartialEq, PartialOrd, Eq, Ord)]
pub struct ImportedTripLeg {
    route: ImportedRouteId,
    origin: ImportedStationId,
    destination: ImportedStationId,
    departure: usize,
    arrival: usize,
}

impl ImportedTripLeg {
    pub fn origin(&self) -> &ImportedStationId {
        &self.origin
    }
    pub fn destination(&self) -> &ImportedStationId {
        &self.destination
    }
    pub fn route(&self) -> &ImportedRouteId {
        &self.route
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
}

/// A schedule is a set of dates for which a particular trip/train will run.
#[derive(Debug, Clone, Constructor, PartialEq)]
pub struct ImportedSchedule {
    id: ImportedScheduleId,
    dates: Vec<String>,
}

impl ImportedSchedule {
    pub fn id(&self) -> &ImportedScheduleId {
        &self.id
    }
    pub fn dates(&self) -> &[String] {
        &self.dates
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedScheduleId(String);

impl ImportedScheduleId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A route is an abstract concept that group trips sharing some caracteristics (headsign,
/// schedule, etc.). For our domain, it's mostly used as an intermediary way to map a trip leg
/// to its schedule(s).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash, From, Ord)]
pub struct ImportedRouteId(String);

impl ImportedRouteId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// [`TrainDataToImport`] represents the set of data required to ingest train schedules for a
/// particular source.
#[derive(Debug, Clone, Constructor)]
pub struct TrainDataToImport {
    stations: Vec<ImportedStation>,
    legs: Vec<ImportedTripLeg>,
    schedules: Vec<ImportedSchedule>,
    schedules_by_route: HashMap<ImportedRouteId, Vec<ImportedScheduleId>>,
    source: String,
}

impl TrainDataToImport {
    pub fn stations(&self) -> &[ImportedStation] {
        &self.stations
    }
    pub fn trip_legs(&self) -> &[ImportedTripLeg] {
        &self.legs
    }
    pub fn schedules(&self) -> &[ImportedSchedule] {
        &self.schedules
    }
    pub fn schedules_by_route(&self) -> &HashMap<ImportedRouteId, Vec<ImportedScheduleId>> {
        &self.schedules_by_route
    }
    pub fn source(&self) -> &str {
        &self.source
    }
}
