use std::{
    collections::{HashMap, HashSet},
    time::UNIX_EPOCH,
};

use eyre::{Context, Result};
use itertools::Itertools;
use reqwest::{header, Client};
use serde::{Serialize, Serializer};
use tracing::Level;
use url::Url;

use crate::{
    event::{level_to_str, EventWrapper, Fields, Value},
    sender::Sender,
};

fn serialize_level<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    level_to_str(*level).serialize(serializer)
}

#[derive(Serialize)]
struct Line {
    #[serde(rename = "_msg")]
    msg: String,
    #[serde(rename = "_time")]
    time: u128,
    #[serde(serialize_with = "serialize_level")]
    level: Level,
    #[serde(rename = "tracing.target")]
    target: &'static str,
    #[serde(rename = "tracing.module_path", skip_serializing_if = "Option::is_none")]
    module_path: Option<&'static str>,
    #[serde(flatten)]
    fields: Fields,
}

pub struct VictorialogsSender {
    url: Url,
    client: Client,
    stream_fields: HashSet<String>,
    extra_fields: HashMap<String, Value>,
}

impl VictorialogsSender {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            url: Url::parse(url.as_ref()).context("parse url")?,
            client: Client::new(),
            stream_fields: HashSet::new(),
            extra_fields: HashMap::new(),
        })
    }

    pub fn stream_fields<'a>(mut self, fields: impl IntoIterator<Item = &'a str>) -> Self {
        self.stream_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn extra_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra_field_mut(key, value);
        self
    }

    pub fn extra_field_mut(&mut self, key: &str, value: impl Into<Value>) {
        self.extra_fields.insert(key.into(), value.into());
    }
}

impl Sender for VictorialogsSender {
    type Encoded = Vec<u8>;

    fn encode(&self, events: Vec<EventWrapper>) -> Result<Self::Encoded> {
        let data = events
            .into_iter()
            .map(|mut ev| {
                ev.fields.0.extend(self.extra_fields.iter().map(|(k, v)| (k.clone(), v.clone())));
                serde_json::to_string(&Line {
                    time: ev.timestamp.duration_since(UNIX_EPOCH).unwrap().as_nanos(),
                    msg: ev.line,
                    level: ev.level,
                    target: ev.target,
                    module_path: ev.module_path,
                    fields: ev.fields,
                })
                .unwrap()
            })
            .join("\n")
            .into_bytes();

        #[cfg(feature = "zstd")]
        return Ok(zstd::encode_all(&*data, 8)?);
        #[cfg(not(feature = "zstd"))]
        Ok(data)
    }

    async fn send(&mut self, data: &Self::Encoded) -> Result<()> {
        #[derive(Serialize)]
        struct Params {
            #[serde(rename = "_stream_fields", skip_serializing_if = "Option::is_none")]
            stream_fields: Option<String>,
        }

        let mut req =
            self.client.post(self.url.clone()).header(header::CONTENT_TYPE, "application/json");
        req =
            if cfg!(feature = "zstd") { req.header(header::CONTENT_ENCODING, "zstd") } else { req };
        req.query(&Params {
            stream_fields: (!self.stream_fields.is_empty())
                .then(|| self.stream_fields.iter().join(",")),
        })
        .body(data.clone())
        .send()
        .await?
        .error_for_status()?;
        Ok(())
    }
}
