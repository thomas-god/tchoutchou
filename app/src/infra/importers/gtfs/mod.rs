use std::str::FromStr;

use derive_more::{Constructor, From};

pub mod importer;
pub mod parsers;

/// The interface a GTFS data source must implement.
/// Methods return flat, unprocessed rows straight from the CSV files.
/// All semantic transformation (stop grouping, trip expansion, etc.) is the
/// responsibility of [`importer::GTFSImporter`].
pub trait ParseGTFS {
    fn stops(&self) -> &[GTFSStop];
    fn stop_times(&self) -> &[GTFSStopTime];
    fn trips(&self) -> &[GTFSTrip];
    fn calendar_dates(&self) -> &[GTFSCalendarDate];
}

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSServiceId(String);

/// A `GTFSSchedule` represents a set of dates for which a train will run.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct GTFSService {
    id: GTFSServiceId,
    dates: Vec<String>,
}

impl GTFSService {
    pub fn id(&self) -> &GTFSServiceId {
        &self.id
    }
    pub fn dates(&self) -> &[String] {
        &self.dates
    }
}

/// A `GTFSRouteId` represent a set of stops that belong to the same physical train and trip. It can
/// be used amongst other things to find the `GTFSSchedule`s for a given `GTFSTrip`.
#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSRouteId(String);

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSTripId(String);

impl GTFSServiceId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl GTFSRouteId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Location type for a stop row (`location_type` column in `stops.txt`).
///
/// Spec: <https://gtfs.org/documentation/schedule/reference/#stopstxt>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GTFSLocationType {
    /// 0 (or empty) — Stop / Platform. Where passengers board or alight.
    Stop,
    /// 1 — Station. Physical structure containing one or more platforms.
    Station,
    /// 2 — Entrance / Exit. Where passengers enter or exit a station from the street.
    EntranceExit,
    /// 3 — Generic Node. Used to connect pathways inside a station.
    GenericNode,
    /// 4 — Boarding Area. A specific location on a platform.
    BoardingArea,
}

impl FromStr for GTFSLocationType {
    type Err = String;

    /// Parse the CSV value. An empty string is treated as `Stop` (spec default).
    /// Returns `None` for any unrecognised value.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim() {
            "" | "0" => Self::Stop,
            "1" => Self::Station,
            "2" => Self::EntranceExit,
            "3" => Self::GenericNode,
            "4" => Self::BoardingArea,
            _ => return Err(format!("Cannot parse {:?} into GTFSLocationType", s)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, From, Hash, Eq, Ord)]
pub struct GTFSStopId(String);

impl GTFSStopId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A single row from `stops.txt`, faithfully mirroring the CSV columns
/// without any grouping or semantic interpretation.
#[derive(Debug, Clone, PartialEq, Constructor)]
pub struct GTFSStop {
    id: GTFSStopId,
    name: String,
    lat: f64,
    lon: f64,
    location_type: GTFSLocationType,
    /// `None` when the `parent_station` column is blank (i.e. this row is a
    /// top-level station).
    parent_station: Option<GTFSStopId>,
}

impl GTFSStop {
    pub fn id(&self) -> &GTFSStopId {
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
    pub fn location_type(&self) -> GTFSLocationType {
        self.location_type
    }
    pub fn parent_station(&self) -> Option<&GTFSStopId> {
        self.parent_station.as_ref()
    }
}

/// A single row from `stop_times.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSStopTime {
    trip_id: GTFSTripId,
    arrival: usize,
    departure: usize,
    stop_id: GTFSStopId,
    stop_sequence: usize,
}

impl GTFSStopTime {
    pub fn new(
        trip_id: GTFSTripId,
        arrival: usize,
        departure: usize,
        stop_id: GTFSStopId,
        stop_sequence: usize,
    ) -> Self {
        Self {
            trip_id,
            arrival,
            departure,
            stop_id,
            stop_sequence,
        }
    }
    pub fn trip_id(&self) -> &GTFSTripId {
        &self.trip_id
    }
    pub fn arrival(&self) -> usize {
        self.arrival
    }
    pub fn departure(&self) -> usize {
        self.departure
    }
    pub fn stop_id(&self) -> &GTFSStopId {
        &self.stop_id
    }
    pub fn stop_sequence(&self) -> usize {
        self.stop_sequence
    }
}

/// A single row from `trips.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSTrip {
    trip_id: GTFSTripId,
    route_id: GTFSRouteId,
    service_id: GTFSServiceId,
}

impl GTFSTrip {
    pub fn new(trip_id: GTFSTripId, route_id: GTFSRouteId, service_id: GTFSServiceId) -> Self {
        Self {
            trip_id,
            route_id,
            service_id,
        }
    }
    pub fn trip_id(&self) -> &GTFSTripId {
        &self.trip_id
    }
    pub fn route_id(&self) -> &GTFSRouteId {
        &self.route_id
    }
    pub fn service_id(&self) -> &GTFSServiceId {
        &self.service_id
    }
}

/// Exception type for a calendar-dates row (`exception_type` column in `calendar_dates.txt`).
///
/// Spec: <https://gtfs.org/documentation/schedule/reference/#calendar_datestxt>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GTFSExceptionType {
    /// 1 — Service has been added for the specified date.
    ServiceAdded,
    /// 2 — Service has been removed for the specified date.
    ServiceRemoved,
}

impl FromStr for GTFSExceptionType {
    type Err = String;

    /// Parse the CSV value. Returns `None` for any unrecognised value.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim() {
            "1" => Self::ServiceAdded,
            "2" => Self::ServiceRemoved,
            _ => return Err(format!("Cannot parse {:?} into GTFSExceptionType", s)),
        })
    }
}

/// A single row from `calendar_dates.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSCalendarDate {
    service_id: GTFSServiceId,
    date: String,
    exception_type: GTFSExceptionType,
}

impl GTFSCalendarDate {
    pub fn new(service_id: GTFSServiceId, date: String, exception_type: GTFSExceptionType) -> Self {
        Self {
            service_id,
            date,
            exception_type,
        }
    }
    pub fn service_id(&self) -> &GTFSServiceId {
        &self.service_id
    }
    pub fn date(&self) -> &str {
        &self.date
    }
    pub fn exception_type(&self) -> GTFSExceptionType {
        self.exception_type
    }
}
