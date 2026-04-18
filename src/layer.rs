use std::{cell::RefCell, future::Future, io::Write, time::SystemTime};

use tracing::{
    span::{Attributes, Record},
    Event, Id, Subscriber,
};
use tracing_subscriber::{
    fmt,
    fmt::{FormatEvent, FormatFields, MakeWriter},
    layer::Context,
    registry::LookupSpan,
};

use crate::{
    event::{EventWrapper, Fields},
    sender::{sender_task, Sender, CAPACITY},
};

#[derive(Clone, Copy)]
struct BufWriter;

impl BufWriter {
    thread_local! {
        static BUF: RefCell<Vec<u8>> = const { RefCell::new(vec![]) };
    }
}

impl<'a> MakeWriter<'a> for BufWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        *self
    }
}

impl Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Self::BUF.with_borrow_mut(|vec| vec.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct Layer<S, N, E> {
    sender: flume::Sender<EventWrapper>,
    fmt_layer: fmt::Layer<S, N, E, BufWriter>,
}

impl<S, N, E> Layer<S, N, E> {
    pub fn new<Sn: Sender>(
        sender: Sn, fmt_layer: fmt::Layer<S, N, E>,
    ) -> (Self, impl Future<Output = ()>) {
        let (tx, rx) = flume::bounded(CAPACITY);
        (Self { sender: tx, fmt_layer: fmt_layer.with_writer(BufWriter) }, sender_task(rx, sender))
    }
}

impl<S, N, E> tracing_subscriber::Layer<S> for Layer<S, N, E>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
    E: FormatEvent<S, N> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        {
            let span = ctx.span(id).expect("Span not found");
            let mut fields = Fields::default();
            attrs.record(&mut fields);
            span.extensions_mut().replace(fields);
        }

        self.fmt_layer.on_new_span(attrs, id, ctx);
    }

    fn on_record(&self, span: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        {
            let span = ctx.span(span).expect("Span not found");
            values.record(span.extensions_mut().get_mut::<Fields>().expect("Fields not found"));
        }

        self.fmt_layer.on_record(span, values, ctx);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().module_path().unwrap_or("").starts_with(env!("CARGO_CRATE_NAME")) {
            return;
        }

        let mut fields = ctx.event_scope(event).into_iter().flat_map(|s| s.from_root()).fold(
            Fields::default(),
            |mut acc, f| {
                acc.0.extend(
                    f.extensions()
                        .get::<Fields>()
                        .expect("no Fields on span")
                        .0
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
                acc
            },
        );

        event.record(&mut fields);

        self.fmt_layer.on_event(event, ctx);
        let mut line = String::from_utf8(BufWriter::BUF.take()).unwrap();
        line.truncate(line.trim_end().len());

        let _ = self.sender.try_send(EventWrapper {
            timestamp: SystemTime::now(),
            line,

            level: *event.metadata().level(),
            target: event.metadata().target(),
            module_path: event.metadata().module_path(),

            fields,
        });
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        self.fmt_layer.on_enter(id, ctx)
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        self.fmt_layer.on_exit(id, ctx)
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.fmt_layer.on_close(id, ctx)
    }
}
