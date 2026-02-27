use std::collections::HashMap;

use derive_more::{Constructor, From};

pub mod importer;
pub mod parser;

pub trait ParseGTFS {
    fn stations(&self) -> &[GTFSStation];
    fn trips(&self) -> &[GTFSTripLeg];
    fn schedules(&self) -> &[GTFSService];
    fn schedules_by_route(&self) -> &HashMap<GTFSRouteId, Vec<GTFSServiceId>>;
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
#[derive(Debug, Clone, PartialEq, Constructor)]
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

impl GTFSLocationType {
    /// Parse the raw CSV value. An empty string is treated as `Stop` (spec default).
    /// Returns `None` for any unrecognised value.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "" | "0" => Some(Self::Stop),
            "1" => Some(Self::Station),
            "2" => Some(Self::EntranceExit),
            "3" => Some(Self::GenericNode),
            "4" => Some(Self::BoardingArea),
            _ => None,
        }
    }
}

/// A single raw row from `stops.txt`, faithfully mirroring the CSV columns
/// without any grouping or semantic interpretation.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSRawStop {
    id: GTFSStopId,
    name: String,
    lat: f64,
    lon: f64,
    location_type: GTFSLocationType,
    /// `None` when the `parent_station` column is blank (i.e. this row is a
    /// top-level station).
    parent_station: Option<GTFSStopId>,
}

impl GTFSRawStop {
    pub fn new(
        id: GTFSStopId,
        name: String,
        lat: f64,
        lon: f64,
        location_type: GTFSLocationType,
        parent_station: Option<GTFSStopId>,
    ) -> Self {
        Self {
            id,
            name,
            lat,
            lon,
            location_type,
            parent_station,
        }
    }
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

/// A single raw row from `stop_times.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSRawStopTime {
    trip_id: GTFSTripId,
    arrival: usize,
    departure: usize,
    stop_id: GTFSStopId,
    stop_sequence: usize,
}

impl GTFSRawStopTime {
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

/// A single raw row from `trips.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSRawTrip {
    trip_id: GTFSTripId,
    route_id: GTFSRouteId,
    service_id: GTFSServiceId,
}

impl GTFSRawTrip {
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

impl GTFSExceptionType {
    /// Parse the raw CSV value. Returns `None` for any unrecognised value.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "1" => Some(Self::ServiceAdded),
            "2" => Some(Self::ServiceRemoved),
            _ => None,
        }
    }
}

/// A single raw row from `calendar_dates.txt`.
#[derive(Debug, Clone, PartialEq)]
pub struct GTFSRawCalendarDate {
    service_id: GTFSServiceId,
    date: String,
    exception_type: GTFSExceptionType,
}

impl GTFSRawCalendarDate {
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
