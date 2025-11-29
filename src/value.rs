use std::collections::HashMap;

use crate::list::ListState;
use crate::skiplist::SkipList;

#[derive(Debug)]
pub enum Value {
    String(Vec<u8>),
    List(ListState),
    ZSet(SkipList),
    Hash(HashMap<String, Vec<u8>>),
}

impl Value {
    pub fn as_string(&self) -> Option<&[u8]> {
        match self {
            Value::String(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, Vec<u8>>> {
        match self {
            Value::Hash(ref mut h) => Some(h),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut ListState> {
        match self {
            Value::List(ref mut l) => Some(l),
            _ => None,
        }
    }

    pub fn as_zset_mut(&mut self) -> Option<&mut SkipList> {
        match self {
            Value::ZSet(ref mut zs) => Some(zs),
            _ => None,
        }
    }
}