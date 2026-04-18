use std::{collections::HashMap, fmt::Debug, time::SystemTime};

use serde::Serialize;
use tracing::{
    field::{Field, Visit},
    Level,
};

#[derive(Debug, Clone, derive_more::Display, derive_more::From, Serialize)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Int(i128),
    Uint(u128),
    Float(f64),
    Bool(bool),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

#[derive(Clone, Default, Serialize)]
#[serde(transparent)]
pub struct Fields(pub HashMap<String, Value>);

impl Visit for Fields {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), Value::Float(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_i128(field, value as i128)
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_u128(field, value as u128);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.0.insert(field.name().into(), Value::Int(value));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0.insert(field.name().into(), Value::Uint(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), Value::Bool(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), Value::String(value.into()));
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.0.insert(field.name().into(), Value::String(hex::encode(value)));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().into(), Value::String(format!("{value:?}")));
    }
}

pub struct EventWrapper {
    pub timestamp: SystemTime,
    pub line: String,

    pub level: Level,
    pub target: &'static str,
    pub module_path: Option<&'static str>,

    pub fields: Fields,
}

pub(crate) fn level_to_str(level: Level) -> &'static str {
    match level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}
