//! An in-memory implementationof a session store.
//!
//! This module provides [MemoryStore], an implementation of [Store]
//! to be used for testing and development. It is not optimized for production
//! and thus you should use another store to use it in the real world.

use std::{collections::HashMap, time::Duration};

use rocket::tokio::sync::{Mutex, RwLock};
use time::OffsetDateTime;

use crate::{SessionResult, Store};

/// An in memory implementation of a session store using hashmaps.
/// Do note that this implementation is just for testing purposes,
/// and should not be used in any real world application.
pub struct MemoryStore<T> {
	map: RwLock<HashMap<String, Mutex<MemoryStoreFrame<T>>>>,
}

struct MemoryStoreFrame<T> {
	value: T,
	expiry: OffsetDateTime,
}

impl<T> Default for MemoryStore<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> MemoryStore<T> {
	/// Create a new in-memory store
	pub fn new() -> Self {
		Self {
			map: RwLock::default(),
		}
	}
}

#[rocket::async_trait]
impl<T> Store for MemoryStore<T>
where
	T: Send + Sync + Clone,
{
	type Value = T;

	async fn get(&self, id: &str) -> SessionResult<Option<Self::Value>> {
		let lock = self.map.read().await;
		if let Some(frame) = lock.get(id) {
			let frame_lock = frame.lock().await;
			if frame_lock.expiry >= OffsetDateTime::now_utc() {
				return Ok(Some(frame_lock.value.clone()));
			};
		};
		Ok(None)
	}

	async fn set(&self, id: &str, value: Self::Value, expiry: Duration) -> SessionResult<()> {
		let mut lock = self.map.write().await;
		let frame = MemoryStoreFrame {
			value,
			expiry: OffsetDateTime::now_utc() + expiry,
		};
		lock.insert(id.into(), Mutex::new(frame));

		Ok(())
	}

	async fn touch(&self, id: &str, duration: Duration) -> SessionResult<()> {
		let lock = self.map.read().await;
		if let Some(frame) = lock.get(id) {
			let mut frame_lock = frame.lock().await;
			frame_lock.expiry = OffsetDateTime::now_utc() + duration;
		};
		Ok(())
	}

	async fn remove(&self, id: &str) -> SessionResult<()> {
		let mut lock = self.map.write().await;
		lock.remove(id);

		Ok(())
	}

	async fn expires(&self, id: &str) -> Option<OffsetDateTime> {
		let lock = self.map.read().await;
		let frame = lock.get(id)?;
		let frame_lock = frame.lock().await;
		Some(frame_lock.expiry)
	}
}
