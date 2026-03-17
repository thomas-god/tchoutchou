use derive_more::{AsRef, Constructor, Deref, From, FromStr};

pub mod destinations;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, From, Deref)]
pub struct CityId(i64);

#[derive(Debug, Clone, From, FromStr, Constructor, PartialEq, PartialOrd, AsRef, Deref)]
#[from(&str, String)]
#[as_ref(str, String)]
pub struct CityName(String);

#[derive(Debug, Clone, From, FromStr, Constructor, PartialEq, PartialOrd, AsRef, Deref)]
#[from(&str, String)]
#[as_ref(str, String)]
pub struct CityCountry(String);

#[derive(Debug, Clone, Constructor)]
pub struct City {
    id: CityId,
    name: CityName,
    country: CityCountry,
    lat: f64,
    lon: f64,
    parent: Option<CityId>,
}

impl City {
    pub fn id(&self) -> &CityId {
        &self.id
    }
    pub fn name(&self) -> &CityName {
        &self.name
    }
    pub fn country(&self) -> &CityCountry {
        &self.country
    }
    pub fn lat(&self) -> f64 {
        self.lat
    }
    pub fn lon(&self) -> f64 {
        self.lon
    }
    pub fn parent(&self) -> &Option<CityId> {
        &self.parent
    }
}
