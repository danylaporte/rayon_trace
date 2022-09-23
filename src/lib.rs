use rayon::iter::plumbing::*;
use rayon::iter::*;
use std::{any::Any, cell::Cell};
use tracing::{
    error,
    span::{EnteredSpan, Id},
    Span,
};

pub fn install_rayon_panic_handler() {
    rayon::ThreadPoolBuilder::new()
        .panic_handler(rayon_panic_handler)
        .build_global()
        .unwrap();
}

pub fn rayon_panic_handler(err: Box<dyn Any + Send + 'static>) {
    let span = Span::current();

    match err.downcast::<String>() {
        Ok(s) => error!(parent: span, error = %s, "panic"),
        Err(_) => error!(parent: span, error = "unknown panic error.", "panic"),
    }
}

pub trait RayonTraceExt: ParallelIterator {
    fn trace(self, name: &'static str) -> Trace<Self> {
        Trace { iter: self, name }
    }
}

impl<T> RayonTraceExt for T where T: ParallelIterator {}

/// `Trace` is an iterator that logs all tasks created.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct Trace<I: ParallelIterator> {
    iter: I,
    name: &'static str,
}

impl<T, I> ParallelIterator for Trace<I>
where
    I: ParallelIterator<Item = T>,
    T: Send,
{
    type Item = T;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        let start_span = tracing::span!(tracing::Level::TRACE, "drive");
        let father_id = start_span.id();

        let logged_consumer = TraceConsumer {
            left: false,
            base: consumer,
            father_id: Cell::new(father_id.map(|id| id.into_u64())),
            name: self.name,
        };

        let _enter = start_span.enter();
        self.iter.drive_unindexed(logged_consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        self.iter.opt_len()
    }
}

struct TraceConsumer<C> {
    base: C,
    left: bool,
    father_id: Cell<Option<u64>>,
    name: &'static str,
}

impl<T, C> Consumer<T> for TraceConsumer<C>
where
    C: Consumer<T>,
    T: Send,
{
    type Folder = TraceFolder<C::Folder>;
    type Reducer = TraceReducer<C::Reducer>;
    type Result = C::Result;

    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
        let sequential_span = if self.left {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "left")
        } else {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "right")
        };
        let parallel_span =
            tracing::span!(parent: sequential_span.id(), tracing::Level::TRACE, "parallel");
        let entered_sequential_span = sequential_span.entered();
        let (left, right, reducer) = self.base.split_at(index);
        (
            TraceConsumer {
                base: left,
                left: true,
                father_id: Cell::new(parallel_span.id().map(|id| id.into_u64())),
                name: self.name,
            },
            TraceConsumer {
                base: right,
                left: false,
                father_id: Cell::new(parallel_span.id().map(|id| id.into_u64())),
                name: self.name,
            },
            TraceReducer {
                base: reducer,
                _seq_span: entered_sequential_span,
                par_span: parallel_span.entered(),
            },
        )
    }

    fn into_folder(self) -> TraceFolder<C::Folder> {
        let sequential_span = if self.left {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "left")
        } else {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "right")
        };
        let folder_span = tracing::span!(parent: sequential_span.id(), tracing::Level::TRACE, "fold", label=self.name);
        let entered_seq = sequential_span.entered();
        TraceFolder {
            base: self.base.into_folder(),
            span: folder_span.entered(),
            outer_span: entered_seq,
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<T, C> UnindexedConsumer<T> for TraceConsumer<C>
where
    C: UnindexedConsumer<T>,
    T: Send,
{
    fn split_off_left(&self) -> Self {
        TraceConsumer {
            left: true,
            base: self.base.split_off_left(),
            father_id: self.father_id.clone(),
            name: self.name,
        }
    }
    fn to_reducer(&self) -> TraceReducer<C::Reducer> {
        let sequential_span = if self.left {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "left")
        } else {
            tracing::span!(parent: self.father_id.get().map(Id::from_u64), tracing::Level::TRACE, "right")
        };
        let entered_sequential_span = sequential_span.entered();
        let parallel_span =
            tracing::span!(parent: entered_sequential_span.id(), tracing::Level::TRACE, "parallel");
        self.father_id
            .set(parallel_span.id().map(|id| id.into_u64()));
        TraceReducer {
            base: self.base.to_reducer(),
            _seq_span: entered_sequential_span,
            par_span: parallel_span.entered(),
        }
    }
}

struct TraceFolder<F> {
    base: F,
    span: EnteredSpan,
    outer_span: EnteredSpan,
}

impl<T, F> Folder<T> for TraceFolder<F>
where
    F: Folder<T>,
    T: Send,
{
    type Result = F::Result;

    fn consume(self, item: T) -> Self {
        TraceFolder {
            base: self.base.consume(item),
            span: self.span,
            outer_span: self.outer_span,
        }
    }

    fn complete(self) -> F::Result {
        self.base.complete()
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

struct TraceReducer<R> {
    base: R,
    par_span: EnteredSpan,
    _seq_span: EnteredSpan,
}

impl<Result, R: Reducer<Result>> Reducer<Result> for TraceReducer<R> {
    fn reduce(self, left: Result, right: Result) -> Result {
        std::mem::drop(self.par_span);
        self.base.reduce(left, right)
    }
}
