use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Tablet {
    pub filename: String,
    pub thing: String,
    pub trait_: String,
    pub thing_is_first: bool,
    pub trait_is_first: bool,
    pub base: String,
    pub trait_is_regex: bool,
    pub passthrough: bool,
    pub querying: bool,
    pub eager: bool,
    pub accumulating: bool,
}
