use super::coalescing::{combine, Combined};
use super::{Event, CLIENT_QUEUE_CAPACITY, CLIENT_QUEUE_LIMIT};
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
    uncoalesced: usize,
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
            uncoalesced: 0,
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

        if let Some(slot) = &event.slot
            && let Some(pending) = queue
                .events
                .iter_mut()
                .rev()
                .find(|pending| pending.slot.as_ref() == Some(slot))
        {
            match combine(slot.class, &pending.value, &event.value) {
                Some(Combined::Incoming) => {
                    *pending = event;
                    return true;
                }
                Some(Combined::Rebuilt(combined)) => match Event::new(event.sequence, combined) {
                    Ok(combined) => {
                        *pending = combined;
                        return true;
                    }
                    Err(error) => {
                        tracing::error!(%error, "could not serialize a combined event");
                    }
                },
                None => {}
            }
        }

        if queue.uncoalesced >= CLIENT_QUEUE_CAPACITY || queue.events.len() >= CLIENT_QUEUE_LIMIT {
            queue.closed = true;
            queue.overflowed = true;
            queue.events.clear();
            queue.uncoalesced = 0;
            drop(queue);
            shared.waker.wake();
            tracing::warn!("event subscriber disconnected after its outbound queue filled");
            return false;
        }

        if event.slot.is_none() {
            queue.uncoalesced += 1;
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
            if event.slot.is_none() {
                queue.uncoalesced -= 1;
            }

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
