use crate::Entry;
use crate::IntoValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;
use std::fmt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct State {
    pub entry: Option<Entry>,
    pub query: Option<Entry>,
    pub fst: Option<String>,
    pub is_match: bool,
    pub match_map: Option<HashMap<String, bool>>,
    pub has_match: bool,
    pub thing_querying: Option<String>,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let state_value = self.clone().into_value();

        let state_string = serde_json::to_string_pretty(&state_value).expect("unreachable");

        write!(f, "{}", state_string)
    }
}

impl IntoValue for State {
    fn into_value(self) -> Value {
        let mut value: Value = json!({});

        value["fst"] = match self.fst {
            None => Value::Null,
            Some(s) => s.into(),
        };

        value["isMatch"] = self.is_match.into();

        value["hasMatch"] = self.has_match.into();

        value["matchMap"] = match self.match_map {
            None => Value::Null,
            Some(m) => {
                let map_string = serde_json::to_string(&m).expect("unreachable");

                let map_value = serde_json::from_str(&map_string).expect("unreachable");

                map_value
            }
        };

        value["thingQuerying"] = match self.thing_querying {
            None => Value::Null,
            Some(s) => s.into(),
        };

        value["entry"] = match self.entry {
            None => Value::Null,
            Some(e) => e.into_value(),
        };

        value["query"] = match self.query {
            None => Value::Null,
            Some(q) => q.into_value(),
        };

        value
    }
}
