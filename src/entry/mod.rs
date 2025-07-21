mod into_value;
pub mod mow;
pub mod sow;
mod try_from;
use crate::Grain;
use crate::IntoValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub base: String,
    pub base_value: Option<String>,
    pub leader_value: Option<String>,
    pub leaves: HashMap<String, Vec<Entry>>,
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.clone().into_value())
    }
}

impl Entry {
    pub fn mow(&self, trait_: &str, thing: &str) -> Vec<Grain> {
        mow::mow(self, trait_, thing)
    }

    pub fn sow(&self, grain: &Grain, trait_: &str, thing: &str) -> Entry {
        sow::sow(self, grain, trait_, thing)
    }
}
