mod into_value;
mod try_from;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Grain {
    pub base: String,
    pub base_value: Option<String>,
    pub leaf: String,
    pub leaf_value: Option<String>,
}
