use super::{CLIENT_QUEUE_CAPACITY, Event};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

#[derive(Debug)]
struct Queue {
    events: VecDeque<Event>,
    closed: bool,
    overflowed: bool,
}

struct Shared {
    queue: Mutex<Queue>,
    waker: AtomicWaker,
}

pub struct EventSender {
    shared: Weak<Shared>,
}

pub struct EventReceiver {
    shared: Arc<Shared>,
}

pub fn event_queue() -> (EventSender, EventReceiver) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(Queue {
            events: VecDeque::new(),
            closed: false,
            overflowed: false,
        }),
        waker: AtomicWaker::new(),
    });

    (
        EventSender {
            shared: Arc::downgrade(&shared),
        },
        EventReceiver { shared },
    )
}

impl EventSender {
    pub fn is_connected(&self) -> bool {
        self.shared
            .upgrade()
            .is_some_and(|shared| shared.queue.lock().is_ok_and(|queue| !queue.closed))
    }

    pub fn send(&self, event: Event) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };

        let Ok(mut queue) = shared.queue.lock() else {
            shared.waker.wake();
            return false;
        };

        if queue.closed {
            return false;
        }

        if let Some(key) = event.meter_key() {
            for pending in queue.events.iter_mut().rev() {
                match pending.meter_key() {
                    Some(pending_key) if pending_key == key => {
                        *pending = event;
                        return true;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        }

        if queue.events.len() >= CLIENT_QUEUE_CAPACITY {
            queue.closed = true;
            queue.overflowed = true;
            queue.events.clear();
            drop(queue);
            shared.waker.wake();
            tracing::warn!("event subscriber disconnected after its outbound queue filled");
            return false;
        }

        queue.events.push_back(event);
        drop(queue);
        shared.waker.wake();
        true
    }

    pub fn close(&self) {
        if let Some(shared) = self.shared.upgrade() {
            if let Ok(mut queue) = shared.queue.lock() {
                queue.closed = true;
            }

            shared.waker.wake();
        }
    }
}

impl EventReceiver {
    pub fn overflowed(&self) -> bool {
        self.shared.queue.lock().is_ok_and(|queue| queue.overflowed)
    }
}

impl Stream for EventReceiver {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.shared.waker.register(context.waker());

        let Ok(mut queue) = self.shared.queue.lock() else {
            return Poll::Ready(None);
        };

        if let Some(event) = queue.events.pop_front() {
            return Poll::Ready(Some(event));
        }

        if queue.closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for EventSender {
    fn drop(&mut self) {
        self.close();
    }
}
