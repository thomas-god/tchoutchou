use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    app::schedule::{DestinationsCache, GraphCache},
    domain::optim::{City, CityId, Graph, Trip},
};

/// In-memory, thread-safe [`GraphCache`] backed by a [`Mutex`]-protected [`HashMap`].
///
/// All clones share the same underlying storage via [`Arc`], so a `clear` or `insert`
/// performed on one handle is immediately visible to all others.
#[derive(Default, Clone)]
pub struct InMemoryGraphCache {
    inner: Arc<Mutex<HashMap<String, Arc<Graph>>>>,
}

impl InMemoryGraphCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl GraphCache for InMemoryGraphCache {
    fn get(&self, date: &str) -> Option<Arc<Graph>> {
        self.inner.lock().ok()?.get(date).map(Arc::clone)
    }

    fn insert(&self, date: &str, graph: Arc<Graph>) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(date.to_owned(), graph);
        }
    }

    fn clear(&self) {
        if let Ok(mut map) = self.inner.lock() {
            map.clear();
        }
    }
}

/// In-memory, thread-safe [`DestinationsCache`] backed by a [`Mutex`]-protected [`HashMap`].
///
/// Cache key is `(date, city_id)`. All clones share the same underlying storage via [`Arc`].
#[derive(Default, Clone)]
pub struct InMemoryDestinationsCache {
    inner: Arc<Mutex<HashMap<(String, i64), Arc<(Vec<Trip>, Vec<City>)>>>>,
}

impl InMemoryDestinationsCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DestinationsCache for InMemoryDestinationsCache {
    fn get(&self, date: &str, origin: &CityId) -> Option<Arc<(Vec<Trip>, Vec<City>)>> {
        self.inner
            .lock()
            .ok()?
            .get(&(date.to_owned(), origin.as_i64()))
            .map(Arc::clone)
    }

    fn insert(&self, date: &str, origin: &CityId, result: Arc<(Vec<Trip>, Vec<City>)>) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert((date.to_owned(), origin.as_i64()), result);
        }
    }

    fn clear(&self) {
        if let Ok(mut map) = self.inner.lock() {
            map.clear();
        }
    }
}
