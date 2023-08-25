use std::cmp::min;

// 3rd party imports
use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::Row as ScyllaRow;
use scylla::FromUserType;
use scylla::IntoUserType;
use tokio_postgres::Row;

use crate::tools::cql::get_cql_value;

#[derive(Clone, PartialEq, Debug, IntoUserType, FromUserType)]
pub struct Domain {
    name: String,
    evidence: String,
    start_index: i64,
    end_index: i64,
}

impl Domain {
    pub fn new(start_index: i64, end_index: i64, name: String, evidence: String) -> Self {
        Self {
            start_index,
            end_index,
            name,
            evidence,
        }
    }

    pub fn get_start_index(&self) -> &i64 {
        &self.start_index
    }

    pub fn get_end_index(&self) -> &i64 {
        &self.end_index
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_evidence(&self) -> &String {
        &self.evidence
    }
}
