//! Test subscriber with level filters, field updates, and span lifetime tracking.
#![allow(dead_code)]
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    thread::ThreadId,
};
use tracing::{
    Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_core::span::Current;
#[derive(Clone, Debug)]
pub(crate) struct Captured {
    pub(crate) metadata: &'static Metadata<'static>,
    pub(crate) parent: Option<usize>,
    pub(crate) fields: BTreeMap<String, String>,
    references: usize,
}
#[derive(Default)]
pub(crate) struct CaptureState {
    pub(crate) spans: Vec<Captured>,
    pub(crate) events: Vec<Captured>,
    stacks: HashMap<ThreadId, Vec<usize>>,
    pub(crate) records: Vec<(usize, BTreeMap<String, String>)>,
    pub(crate) closed: Vec<usize>,
}
#[derive(Clone, Default)]
pub(crate) struct Capture(
    pub(crate) Arc<Mutex<CaptureState>>,
    Option<Level>,
    Option<&'static str>,
);
impl Capture {
    pub(crate) fn named(name: &'static str) -> Self {
        Self(Arc::default(), None, Some(name))
    }
    pub(crate) fn at(level: Level) -> Self {
        Self(Arc::default(), Some(level), None)
    }
}
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.1.is_none_or(|level| *metadata.level() <= level)
            && self.2.is_none_or(|name| metadata.name() == name)
    }
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut state = self.0.lock().unwrap();
        let parent = attrs
            .parent()
            .map(|id| id.into_u64() as usize - 1)
            .or_else(|| {
                attrs
                    .is_contextual()
                    .then(|| {
                        state
                            .stacks
                            .get(&std::thread::current().id())
                            .and_then(|stack| stack.last().copied())
                    })
                    .flatten()
            });
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        state.spans.push(Captured {
            metadata: attrs.metadata(),
            parent,
            fields,
            references: 1,
        });
        Id::from_u64(state.spans.len() as u64)
    }
    fn record(&self, id: &Id, record: &Record<'_>) {
        let mut fields = BTreeMap::new();
        record.record(&mut Fields(&mut fields));
        let mut state = self.0.lock().unwrap();
        let index = id.into_u64() as usize - 1;
        state.spans[index].fields.extend(fields.clone());
        state.records.push((index, fields));
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut state = self.0.lock().unwrap();
        let parent = event
            .parent()
            .map(|id| id.into_u64() as usize - 1)
            .or_else(|| {
                event
                    .is_contextual()
                    .then(|| {
                        state
                            .stacks
                            .get(&std::thread::current().id())
                            .and_then(|stack| stack.last().copied())
                    })
                    .flatten()
            });
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        state.events.push(Captured {
            metadata: event.metadata(),
            parent,
            fields,
            references: 0,
        });
    }
    fn enter(&self, id: &Id) {
        self.0
            .lock()
            .unwrap()
            .stacks
            .entry(std::thread::current().id())
            .or_default()
            .push(id.into_u64() as usize - 1);
    }
    fn exit(&self, id: &Id) {
        assert_eq!(
            self.0
                .lock()
                .unwrap()
                .stacks
                .entry(std::thread::current().id())
                .or_default()
                .pop(),
            Some(id.into_u64() as usize - 1)
        );
    }
    fn clone_span(&self, id: &Id) -> Id {
        self.0.lock().unwrap().spans[id.into_u64() as usize - 1].references += 1;
        id.clone()
    }
    fn try_close(&self, id: Id) -> bool {
        let mut state = self.0.lock().unwrap();
        let index = id.into_u64() as usize - 1;
        state.spans[index].references -= 1;
        let closed = state.spans[index].references == 0;
        if closed {
            state.closed.push(index);
        }
        closed
    }
    fn current_span(&self) -> Current {
        let state = self.0.lock().unwrap();
        state
            .stacks
            .get(&std::thread::current().id())
            .and_then(|stack| stack.last())
            .map_or_else(Current::none, |&index| {
                Current::new(Id::from_u64(index as u64 + 1), state.spans[index].metadata)
            })
    }
}
