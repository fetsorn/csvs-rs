mod count_leaves;
mod find_crown;
mod get_nesting_level;
mod is_connected;
mod sort_nesting_ascending;
mod sort_nesting_descending;
mod try_from;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Leaves(pub Vec<String>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trunks(pub Vec<String>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Branch {
    pub trunks: Trunks,
    pub leaves: Leaves,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema(pub HashMap<String, Branch>);

impl Schema {
    pub fn is_connected(&self, base: &str, branch: &str) -> bool {
        is_connected::is_connected(self, base, branch)
    }

    pub fn find_crown(&self, base: &str) -> Vec<String> {
        find_crown::find_crown(self, base)
    }

    pub fn count_leaves(&self, branch: &str) -> usize {
        count_leaves::count_leaves(self, branch)
    }

    pub fn get_nesting_level(&self, branch: &str) -> i32 {
        get_nesting_level::get_nesting_level(self, branch)
    }

    pub fn sort_nesting_descending(self) -> impl FnMut(&String, &String) -> Ordering {
        sort_nesting_descending::sort_nesting_descending(self)
    }

    pub fn sort_nesting_ascending(self) -> impl FnMut(&String, &String) -> Ordering {
        sort_nesting_ascending::sort_nesting_ascending(self)
    }
}
