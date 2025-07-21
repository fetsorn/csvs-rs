use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Line {
    pub key: String,
    pub value: String,
}
