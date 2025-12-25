//! Watch and notification types for reactive state updates.
//!
//! Implements from storage.dol:
//! - `storage.watch_event` gene: key, event_type, old_value, new_value, new_version
//! - `storage.watch` trait: pattern-based event streams

use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use tokio_stream::Stream;

/// Type of change that occurred to a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// Key was created (did not exist before).
    Created,
    /// Key was updated (existed and value changed).
    Updated,
    /// Key was deleted.
    Deleted,
}

/// An event representing a change to a key in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchEvent {
    /// The key that changed.
    pub key: String,
    /// Type of change.
    pub event_type: EventType,
    /// Previous value (for updates and deletes).
    pub old_value: Option<Vec<u8>>,
    /// New value (for creates and updates).
    pub new_value: Option<Vec<u8>>,
    /// New version after the change (0 if deleted).
    pub new_version: u64,
}

impl WatchEvent {
    /// Create a new Created event.
    pub fn created(key: impl Into<String>, value: Vec<u8>, version: u64) -> Self {
        Self {
            key: key.into(),
            event_type: EventType::Created,
            old_value: None,
            new_value: Some(value),
            new_version: version,
        }
    }

    /// Create a new Updated event.
    pub fn updated(
        key: impl Into<String>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
        version: u64,
    ) -> Self {
        Self {
            key: key.into(),
            event_type: EventType::Updated,
            old_value: Some(old_value),
            new_value: Some(new_value),
            new_version: version,
        }
    }

    /// Create a new Deleted event.
    pub fn deleted(key: impl Into<String>, old_value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            event_type: EventType::Deleted,
            old_value: Some(old_value),
            new_value: None,
            new_version: 0,
        }
    }
}

/// A stream of watch events for a specific pattern.
pub struct WatchStream {
    receiver: broadcast::Receiver<WatchEvent>,
    pattern: String,
}

impl WatchStream {
    /// Create a new watch stream for the given pattern.
    pub fn new(receiver: broadcast::Receiver<WatchEvent>, pattern: impl Into<String>) -> Self {
        Self {
            receiver,
            pattern: pattern.into(),
        }
    }

    /// Get the pattern this stream is watching.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Check if a key matches this stream's pattern.
    fn matches(&self, key: &str) -> bool {
        if self.pattern.ends_with('*') {
            let prefix = &self.pattern[..self.pattern.len() - 1];
            key.starts_with(prefix)
        } else {
            key == self.pattern
        }
    }
}

impl Stream for WatchStream {
    type Item = WatchEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    if self.matches(&event.key) {
                        return Poll::Ready(Some(event));
                    }
                    // Event doesn't match pattern, continue polling
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    // Register waker and return pending
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Poll::Ready(None);
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // Skip lagged events
                    continue;
                }
            }
        }
    }
}

/// Handle for sending watch events to subscribers.
#[derive(Clone)]
pub struct WatchSender {
    sender: broadcast::Sender<WatchEvent>,
}

impl WatchSender {
    /// Create a new watch sender with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Send an event to all subscribers.
    pub fn send(&self, event: WatchEvent) {
        // Ignore send errors (no subscribers)
        let _ = self.sender.send(event);
    }

    /// Subscribe to events.
    pub fn subscribe(&self, pattern: impl Into<String>) -> WatchStream {
        WatchStream::new(self.sender.subscribe(), pattern)
    }

    /// Get the number of current subscribers.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for WatchSender {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_serialize() {
        let json = serde_json::to_string(&EventType::Created).unwrap();
        assert_eq!(json, "\"Created\"");
    }

    #[test]
    fn test_watch_event_created() {
        let event = WatchEvent::created("/test/key", vec![1, 2, 3], 1);
        assert_eq!(event.key, "/test/key");
        assert_eq!(event.event_type, EventType::Created);
        assert!(event.old_value.is_none());
        assert_eq!(event.new_value, Some(vec![1, 2, 3]));
        assert_eq!(event.new_version, 1);
    }

    #[test]
    fn test_watch_event_updated() {
        let event = WatchEvent::updated("/test/key", vec![1], vec![2], 2);
        assert_eq!(event.event_type, EventType::Updated);
        assert_eq!(event.old_value, Some(vec![1]));
        assert_eq!(event.new_value, Some(vec![2]));
    }

    #[test]
    fn test_watch_event_deleted() {
        let event = WatchEvent::deleted("/test/key", vec![1, 2, 3]);
        assert_eq!(event.event_type, EventType::Deleted);
        assert_eq!(event.old_value, Some(vec![1, 2, 3]));
        assert!(event.new_value.is_none());
        assert_eq!(event.new_version, 0);
    }

    #[test]
    fn test_pattern_matching() {
        let sender = WatchSender::new(16);
        let stream = sender.subscribe("/workloads/*");

        assert!(stream.matches("/workloads/abc"));
        assert!(stream.matches("/workloads/def/instances"));
        assert!(!stream.matches("/nodes/abc"));
    }

    #[test]
    fn test_exact_pattern() {
        let sender = WatchSender::new(16);
        let stream = sender.subscribe("/workloads/abc");

        assert!(stream.matches("/workloads/abc"));
        assert!(!stream.matches("/workloads/def"));
        assert!(!stream.matches("/workloads/abc/instances"));
    }
}
