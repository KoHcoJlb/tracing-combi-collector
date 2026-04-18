use std::collections::HashMap;

use eyre::{Result, WrapErr};
use itertools::Itertools;
use prost::Message;
use reqwest::Client;
use tracing::Level;
use url::Url;

use crate::{
    event::{EventWrapper, Value},
    sender::{
        loki::proto::{
            logproto,
            logproto::{EntryAdapter, LabelPairAdapter, StreamAdapter},
        },
        Sender,
    },
};

mod proto {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

pub struct LokiSender {
    url: Url,
    labels: HashMap<String, String>,
    fields: HashMap<String, String>,
    client: Client,
}

fn format_labels(mut labels: HashMap<String, String>, level: Level) -> String {
    let level = match level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    };

    labels.insert("level".into(), level.into());

    let labels =
        labels.into_iter().map(|(k, v)| format!("{}={:?}", k, v)).collect::<Vec<_>>().join(",");

    format!("{{{labels}}}")
}

fn event_to_entry(ev: EventWrapper, fields: &HashMap<String, String>) -> EntryAdapter {
    EntryAdapter {
        timestamp: Some(ev.timestamp.into()),
        line: ev.line,
        structured_metadata: ev
            .fields
            .0
            .into_iter()
            .chain(fields.iter().map(|(key, value)| (key.clone(), Value::String(value.clone()))))
            .map(|(k, v)| LabelPairAdapter { name: k.clone(), value: v.to_string() })
            .collect(),
        parsed: Default::default(),
    }
}

impl Sender for LokiSender {
    type Encoded = Vec<u8>;

    fn encode(&self, events: Vec<EventWrapper>) -> Result<Self::Encoded> {
        let streams = events
            .into_iter()
            .into_group_map_by(|e| e.level)
            .into_iter()
            .map(|(level, events)| StreamAdapter {
                labels: format_labels(self.labels.clone(), level),
                entries: events.into_iter().map(|ev| event_to_entry(ev, &self.fields)).collect(),
                ..Default::default()
            })
            .collect();

        let req = logproto::PushRequest { streams };

        Ok(snap::raw::Encoder::new().compress_vec(&req.encode_to_vec())?)
    }

    async fn send(&mut self, data: &Self::Encoded) -> Result<()> {
        self.client
            .post(self.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-snappy")
            .body(data.clone())
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

impl LokiSender {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            url: Url::parse(url.as_ref()).context("parse url")?,
            labels: Default::default(),
            fields: Default::default(),
            client: Client::new(),
        })
    }

    pub fn add_label(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.add_label_mut(key, value);
        self
    }

    pub fn add_label_mut(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> &mut Self {
        self.labels.insert(key.as_ref().into(), value.as_ref().into());
        self
    }

    pub fn add_field(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.add_field_mut(key, value);
        self
    }

    pub fn add_field_mut(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> &mut Self {
        self.fields.insert(key.as_ref().into(), value.as_ref().into());
        self
    }
}
