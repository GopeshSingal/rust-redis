use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, Duration};

use tokio::sync::RwLock;
use tokio::time;

use crate::command::Command;
use crate::resp::Frame;
use crate::value::Value;
use crate::list::ListState;
use crate::skiplist::SkipList;
use crate::errors::RedisError;

#[derive(Debug)]
pub struct Db {
    inner: RwLock<HashMap<String, Value>>,
    ttl: RwLock<HashMap<String, Instant>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            ttl: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_inner(&self) -> tokio::sync::RwLockReadGuard<'_, HashMap<String, Value>> {
        self.inner.read().await
    }

    pub async fn get_inner_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, Value>> {
        self.inner.write().await
    }

    pub async fn get_ttl(&self) -> tokio::sync::RwLockReadGuard<'_, HashMap<String, Instant>> {
        self.ttl.read().await
    }

    pub async fn get_ttl_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, Instant>> {
        self.ttl.write().await
    }

    async fn is_expired(&self, key: &str) -> bool {
        let ttl = self.ttl.read().await;
        if let Some(exp_at) = ttl.get(key) {
            if Instant::now() >= *exp_at {
                return true;
            }
        }
        false
    }

    async fn purge_expired(&self, key: &str) {
        let mut ttl = self.ttl.write().await;
        let mut inner = self.inner.write().await;

        ttl.remove(key);
        inner.remove(key);
    }

    async fn check_and_purge(&self, key: &str) -> bool {
        if self.is_expired(key).await {
            self.purge_expired(key).await;
            true
        } else {
            false
        }
    }

    pub async fn apply(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Ping => Frame::Simple("PONG".to_string()),
            Command::Get(key) => self.get(&key).await,
            Command::Set(key, val) => self.set(key, val).await,
            Command::LPush(key, vals) => self.lpush(key, vals).await,
            Command::RPop(key) => self.rpop(&key).await,
            Command::BRPop(key, timeout) => self.brpop(key, timeout).await,
            Command::Del(key) => self.del(&key).await,
            Command::Expire(key, secs) => self.expire(key, secs).await,
            Command::Ttl(key) => self.ttl(&key).await,
            Command::ZAdd(key, score, member) => self.zadd(key, score, member).await,
            Command::ZRangeByScore(key, min, max) => self.zrange_by_score(key, min, max).await,
            Command::ZRem(key, member) => self.zrem(key, member).await,
            Command::HSet(key, field, value) => self.hset(key, field, value).await,
            Command::HGet(key, field) => self.hget(key, field).await,
            Command::HDel(key, fields) => self.hdel(key, fields).await,
            Command::HGetAll(key) => self.hgetall(key).await,
            Command::HMGet(key, fields) => self.hmget(key, fields).await,
            Command::HExists(key, field) => self.hexists(key, field).await,
            Command::HLen(key) => self.hlen(key).await,
            Command::HKeys(key) => self.hkeys(key).await,
            Command::HVals(key) => self.hvals(key).await,
        }
    }

    async fn get(&self, key: &str) -> Frame {
        if self.check_and_purge(key).await {
            return Frame::Null;
        }
        let inner = self.inner.read().await;
        match inner.get(key) {
            Some(Value::String(v)) => Frame::Bulk(v.clone()),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn set(&self, key: String, val: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;
        let mut inner = self.inner.write().await;
        inner.insert(key, Value::String(val));
        Frame::Simple("OK".into())
    }

    async fn lpush(&self, key: String, vals: Vec<Vec<u8>>) -> Frame {
        self.check_and_purge(&key).await;
        let mut inner = self.inner.write().await;
        let entry = inner.entry(key).or_insert_with(|| Value::List(ListState::new()));

        match entry {
            Value::List(list) => {
                for v in vals {
                    list.data.push_front(v);
                }
                list.notify.notify_one();
                Frame::Integer(list.data.len() as i64)
            }
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        }
    }

    async fn rpop(&self, key: &str) -> Frame {
        if self.check_and_purge(key).await {
            return Frame::Null;
        }
        let mut inner = self.inner.write().await;
        let value_opt = inner.get_mut(key);
        if let Some(value) = value_opt {
            if let Some(list) = value.as_list_mut() {
                if let Some(v) = list.data.pop_back() {
                    return Frame::Bulk(v);
                } else {
                    return Frame::Null;
                }
            } else {
                return Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into());
            }
        }
        Frame::Null
    }

    async fn brpop(&self, key: String, timeout_secs: usize) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let timeout = Duration::from_secs(timeout_secs as u64);
        let deadline = time::Instant::now() + timeout;

        loop {
            let notify_opt = {
                let mut inner = self.inner.write().await;

                match inner.get_mut(&key) {
                    Some(Value::List(list)) => {
                        if let Some(v) = list.data.pop_back() {
                            return Frame::Array(vec![
                                Frame::Bulk(key.as_bytes().to_vec()),
                                Frame::Bulk(v),
                            ]);
                        }
                        Some(list.notify.clone())
                    }
                    Some(_) => {
                        return Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        );
                    }
                    None => {
                        None
                    }
                }
            };

            if let Some(notify) = notify_opt {
                let now = time::Instant::now();
                if now >= deadline {
                    return Frame::Null;
                }

                let remaining = deadline - now;

                if time::timeout(remaining, notify.notified()).await.is_err() {
                    return Frame::Null;
                }

                if self.check_and_purge(&key).await {
                    return Frame::Null;
                }

                continue;
            }

            let now = time::Instant::now();
            if now >= deadline {
                return Frame::Null;
            }

            let remaining = deadline - now;
            let sleep_dur = remaining.min(Duration::from_millis(10));
            time::sleep(sleep_dur).await;

            if self.check_and_purge(&key).await {
                return Frame::Null;
            }
        }
    }


    async fn del(&self, key: &str) -> Frame {
        if self.check_and_purge(key).await {
            return Frame::Integer(0);
        }
        
        let mut inner = self.inner.write().await;
        let removed = inner.remove(key).is_some();

        let mut ttl = self.ttl.write().await;
        ttl.remove(key);

        Frame::Integer(removed as i64)
    }

    async fn expire(&self, key: String, secs: usize) -> Frame {
        let inner = self.inner.read().await;
        if !inner.contains_key(&key) {
            return Frame::Integer(0);
        }

        drop(inner);

        let mut ttl = self.ttl.write().await;
        ttl.insert(key, Instant::now() + Duration::from_secs(secs as u64));
        Frame::Integer(1)
    }

    async fn ttl(&self, key: &str) -> Frame {
        let inner = self.inner.read().await;
        if !inner.contains_key(key) {
            return Frame::Integer(-2);
        }

        drop(inner);

        let ttl = self.ttl.read().await;
        if let Some(exp_at) = ttl.get(key) {
            let now = Instant::now();

            if now >= *exp_at {
                return Frame::Integer(-2);
            }

            let remaining = (*exp_at - now).as_secs() as i64;
            Frame::Integer(remaining)
        } else {
            Frame::Integer(-1)
        }
    }

    async fn zadd(&self, key: String, score: f64, member: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;
        let mut inner = self.get_inner_mut().await;
        let entry = inner
            .entry(key)
            .or_insert_with(|| Value::ZSet(SkipList::new()));

        match entry {
            Value::ZSet(zset) => {
                zset.insert(score, member);
                Frame::Integer(1)
            }
            _ => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        }
    }

    async fn zrange_by_score(&self, key: String, min: f64, max: f64) -> Frame {
        self.check_and_purge(&key).await;
        let inner = self.get_inner().await;
        let Some(value) = inner.get(&key) else {
            return Frame::Array(vec![]);
        };

        match value {
            Value::ZSet(zset) => {
                let members = zset.range_by_score(min, max);
                let frames = members
                    .into_iter()
                    .map(Frame::Bulk)
                    .collect::<Vec<_>>();
                Frame::Array(frames)
            }
            _ => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        }
    }

    async fn zrem(&self, key: String, member: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;
        let mut inner = self.get_inner_mut().await;
        let Some(value) = inner.get_mut(&key) else {
            return Frame::Integer(0);
        };

        match value {
            Value::ZSet(zset) => {
                let removed = zset.remove_member(&member);
                Frame::Integer(if removed { 1 } else { 0 })
            }
            _ => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        }
    }

    // Hash commands
    async fn hset(&self, key: String, field: String, value: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;

        let entry = inner.entry(key).or_insert_with(|| {
            Value::Hash(HashMap::new())
        });

        match entry {
            Value::Hash(map) => {
                let existed = map.insert(field, value).is_some();
                Frame::Integer(if existed { 0 } else { 1 })
            }
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
    }

    async fn hget(&self, key: String, field: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => {
                match map.get(&field) {
                    Some(val) => Frame::Bulk(val.clone()),
                    None => Frame::Null,
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn hdel(&self, key: String, fields: Vec<String>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::Hash(map)) => {
                let mut removed = 0;
                for f in fields {
                    if map.remove(&f).is_some() {
                        removed += 1;
                    }
                }
                Frame::Integer(removed)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn hgetall(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => {
                let mut arr = Vec::new();
                for (k, v) in map {
                    arr.push(Frame::Bulk(k.as_bytes().to_vec()));
                    arr.push(Frame::Bulk(v.clone()));
                }
                Frame::Array(arr)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    async fn hmget(&self, key: String, fields: Vec<String>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![Frame::Null; fields.len()])
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => {
                let mut arr = Vec::new();
                for f in fields {
                    match map.get(&f) {
                        Some(v) => arr.push(Frame::Bulk(v.clone())),
                        None => arr.push(Frame::Null),
                    }
                }
                Frame::Array(arr)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![Frame::Null; fields.len()]),
        }
    }

    async fn hexists(&self, key: String, field: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) =>
                Frame::Integer(if map.contains_key(&field) { 1 } else { 0 }),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn hlen(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => Frame::Integer(map.len() as i64),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn hkeys(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => {
                let arr = map.keys()
                    .map(|k| Frame::Bulk(k.as_bytes().to_vec()))
                    .collect();
                Frame::Array(arr)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    pub async fn hvals(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }
        
        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Hash(map)) => {
                let arr = map.values()
                    .map(|v| Frame::Bulk(v.clone()))
                    .collect();
                Frame::Array(arr)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }
}