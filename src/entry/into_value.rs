impl IntoValue for Entry {
    fn into_value(self) -> Value {
        let mut value: Value = json!({
            "_": self.base,
        });

        match self.base_value {
            None => (),
            Some(s) => value[self.base] = s.into(),
        }

        match self.leader_value {
            None => (),
            Some(s) => value["__"] = s.into(),
        }

        for (leaf, items) in self.leaves.iter() {
            for entry in items {
                // condense entry to a string if it has no leaves
                let leaf_value: Value = match entry.leaves.is_empty() {
                    true => match &entry.base_value {
                        None => continue,
                        Some(s) => s.to_owned().into(),
                    },
                    false => entry.clone().into_value(),
                };

                value[&leaf] = match value.get(leaf) {
                    None => leaf_value,
                    Some(i) => match i {
                        Value::Null => panic!("unreachable"),
                        Value::Bool(_) => panic!("unreachable"),
                        Value::Number(_) => panic!("unreachable"),
                        Value::String(s) => vec![s.to_owned().into(), leaf_value].into(),
                        Value::Object(o) => vec![o.clone().into(), leaf_value].into(),
                        Value::Array(vs) => [&vs[..], &[leaf_value]].concat().into(),
                    },
                };
            }
        }

        value
    }
}
