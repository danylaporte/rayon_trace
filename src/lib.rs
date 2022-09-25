use backtrace::{Backtrace, BacktraceFrame};
use std::any::Any;
use tracing::{error_span, Span};

pub fn handle_rayon_panic(err: Box<dyn Any + Send>) {
    let span = Span::current();
    log_backtrace(span, err);
}

pub fn install_rayon_panic_handler() {
    rayon::ThreadPoolBuilder::new()
        .panic_handler(handle_rayon_panic)
        .build_global()
        .unwrap();
}

fn log_backtrace(span: Span, err: Box<dyn Any + Send>) -> Backtrace {
    let frames: Vec<BacktraceFrame> = Backtrace::new().into();

    // skip beginning of the backtrace which is not useful
    let backtrace: Backtrace = frames.into_iter().skip(8).collect::<Vec<_>>().into();

    let cause = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("<cause unknown>");

    error_span!(
        parent: &span,
        "panic",
        cause = %cause,
        trace = ?backtrace,
    );

    backtrace
}
