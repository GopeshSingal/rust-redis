use std::collections::{HashMap, HashSet};
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
            
            // Keyspace commands
            Command::Expire(key, secs) => self.expire(key, secs).await,
            Command::Ttl(key) => self.ttl(&key).await,
            
            // String commands
            Command::Get(key) => self.get(&key).await,
            Command::Set(key, val) => self.set(key, val).await,
            Command::Del(key) => self.del(&key).await,
            Command::Append(key, val) => self.append(key, val).await,
            Command::StrLen(key) => self.strlen(key).await,
            Command::GetSet(key, val) => self.getset(key, val).await,
            Command::Incr(key) => self.incr(key).await,
            Command::IncrBy(key, amt) => self.incrby(key, amt).await,
            Command::MSet(kvs) => self.mset(kvs).await,
            Command::MGet(keys) => self.mget(keys).await,

            // List commands
            Command::LPush(key, vals) => self.lpush(key, vals).await,
            Command::LPop(key) => self.lpop(key).await,
            Command::RPush(key, vals) => self.rpush(key, vals).await,
            Command::RPop(key) => self.rpop(&key).await,
            Command::LLen(key) => self.llen(key).await,
            Command::LRange(key, s, e) => self.lrange(key, s, e).await,
            Command::LIndex(key, idx) => self.lindex(key, idx).await,
            Command::LSet(key, idx, val) => self.lset(key, idx, val).await,
            Command::LTrim(key, s, e) => self.ltrim(key, s, e).await,
            Command::BRPop(key, timeout) => self.brpop(key, timeout).await,

            // Hash commands
            Command::HSet(key, field, value) => self.hset(key, field, value).await,
            Command::HGet(key, field) => self.hget(key, field).await,
            Command::HDel(key, fields) => self.hdel(key, fields).await,
            Command::HGetAll(key) => self.hgetall(key).await,
            Command::HMGet(key, fields) => self.hmget(key, fields).await,
            Command::HExists(key, field) => self.hexists(key, field).await,
            Command::HLen(key) => self.hlen(key).await,
            Command::HKeys(key) => self.hkeys(key).await,
            Command::HVals(key) => self.hvals(key).await,

            // Set commands
            Command::SAdd(key, members) => self.sadd(key, members).await,
            Command::SRem(key, members) => self.srem(key, members).await,
            Command::SMembers(key) => self.smembers(key).await,
            Command::SIsMember(key, member) => self.sismember(key, member).await,
            Command::SCard(key) => self.scard(key).await,
            Command::SUnion(keys) => self.sunion(keys).await,
            Command::SInter(keys) => self.sinter(keys).await,
            Command::SDiff(keys) => self.sdiff(keys).await,
            
            // Sorted Set commands
            Command::ZAdd(key, score, member) => self.zadd(key, score, member).await,
            Command::ZRem(key, member) => self.zrem(key, member).await,
            Command::ZRange(key, start, end) => self.zrange(key, start, end).await,
            Command::ZRevRange(key, start, end) => self.zrevrange(key, start, end).await,
            Command::ZCard(key) => self.zcard(key).await,
            Command::ZScore(key, member) => self.zscore(key, member).await,
            Command::ZRangeByScore(key, min, max) => self.zrange_by_score(key, min, max).await,
            Command::ZRemRangeByScore(key, min, max) => self.zremrangebyscore(key, min, max).await,
            Command::ZRank(key, member) => self.zrank(key, member).await,
            Command::ZRevRank(key, member) => self.zrevrank(key, member).await,
            Command::ZCount(key, min, max) => self.zcount(key, min, max).await,
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

    async fn append(&self, key: String, val: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;
        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::String(s)) => {
                s.extend_from_slice(&val);
                Frame::Integer(s.len() as i64)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => {
                inner.insert(key.clone(), Value::String(val.clone()));
                Frame::Integer(val.len() as i64)
            }
        }
    }

    async fn strlen(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::String(s)) => Frame::Integer(s.len() as i64),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn getset(&self, key: String, val: Vec<u8>) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;

        let old = match inner.get(&key) {
            Some(Value::String(s)) => Frame::Bulk(s.clone()),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        };

        inner.insert(key, Value::String(val));
        old
    }

    async fn incr(&self, key: String) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;

        let curr = match inner.get(&key) {
            Some(Value::String(s)) => {
                let s = match std::str::from_utf8(s) {
                    Ok(v) => v,
                    Err(_) => {
                        return Frame::Error("ERR value is not an integer".into());
                    }
                };
                match s.parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => {
                        return Frame::Error("ERR value is not an integer".into());
                    }
                }
            }
            Some(_) => return Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => 0,
        };

        let new_val = curr + 1;
        inner.insert(key, Value::String(new_val.to_string().into_bytes()));
        Frame::Integer(new_val)
    }

    async fn incrby(&self, key: String, amt: i64) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;

        let curr = match inner.get(&key) {
            Some(Value::String(s)) => {
                let s = match std::str::from_utf8(s) {
                    Ok(v) => v,
                    Err(_) => {
                        return Frame::Error("ERR value is not an integer".into());
                    }
                };
                match s.parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => {
                        return Frame::Error("ERR value is not an integer".into());
                    }
                }
            }
            Some(_) => return Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => 0,
        };

        let new_val = curr + amt;
        inner.insert(key, Value::String(new_val.to_string().into_bytes()));
        Frame::Integer(new_val)
    }

    async fn mset(&self, kvs: Vec<(String, Vec<u8>)>) -> Frame {
        let mut inner = self.inner.write().await;

        for (k, v) in kvs {
            inner.insert(k, Value::String(v));
        }

        Frame::Simple("OK".into())
    }

    async fn mget(&self, keys: Vec<String>) -> Frame {
        let inner = self.inner.read().await;

        let mut arr = Vec::new();

        for k in keys {
            match inner.get(&k) {
                Some(Value::String(s)) => arr.push(Frame::Bulk(s.clone())),
                Some(_) => arr.push(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())),
                None => arr.push(Frame::Null),
            }
        }

        Frame::Array(arr)
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

    async fn lpop(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::List(list)) => {
                if let Some(v) = list.data.pop_front() {
                    Frame::Bulk(v)
                } else {
                    Frame::Null
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn rpush(&self, key: String, vals: Vec<Vec<u8>>) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;
        let entry = inner.entry(key).or_insert_with(|| Value::List(ListState::new()));

        match entry {
            Value::List(list) => {
                for v in vals {
                    list.data.push_back(v);
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

    async fn llen(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::List(list)) => Frame::Integer(list.data.len() as i64),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn lindex(&self, key: String, index: i64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::List(list)) => {
                let len = list.data.len() as i64;

                let idx = if index < 0 {
                    len + index
                } else {
                    index
                };

                if idx < 0 || idx >= len {
                    Frame::Null
                } else {
                    Frame::Bulk(list.data[idx as usize].clone())
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn lset(&self, key: String, index: i64, val: Vec<u8>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Error("ERR no such key".into());
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::List(list)) => {
                let len = list.data.len() as i64;

                let idx = if index < 0 {
                    len + index
                } else {
                    index
                };

                if idx < 0 || idx >= len {
                    return Frame::Error("ERR index out of range".into());
                }

                list.data[idx as usize] = val;
                Frame::Simple("OK".into())
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Error("ERR no such key".into()),
        }
    }

    async fn lrange(&self, key: String, start: i64, end: i64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::List(list)) => {
                let len = list.data.len() as i64;

                let s = if start < 0 { len + start } else { start }.max(0);
                let e = if end < 0 { len + end } else { end }.max(0);

                if s > e || s >= len {
                    return Frame::Array(vec![]);
                }

                let e = e.min(len - 1);

                let result = list.data
                    .iter()
                    .skip(s as usize)
                    .take((e - s + 1) as usize)
                    .cloned()
                    .map(Frame::Bulk)
                    .collect::<Vec<_>>();

                Frame::Array(result)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    async fn ltrim(&self, key: String, start: i64, end: i64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Simple("OK".into());
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::List(list)) => {
                let len = list.data.len() as i64;

                let s = if start < 0 { len + start } else { start }.max(0);
                let e = if end < 0 { len + end } else { end }.max(0);

                if s > e || s >= len {
                    list.data.clear();
                    return Frame::Simple("OK".into());
                }

                let e = e.min(len - 1);

                list.data = list.data
                    .iter()
                    .skip(s as usize)
                    .take((e - s + 1) as usize)
                    .cloned()
                    .collect();

                Frame::Simple("OK".into())
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Simple("OK".into()),
        }
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

    async fn zrange(&self, key: String, start: i64, end: i64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                let len = zset.len() as i64;
                if len == 0 {
                    return Frame::Array(vec![]);
                }

                let mut s = if start < 0 { len + start } else { start };
                let mut e = if end < 0 { len + end } else { end };

                if s < 0 { s = 0; }
                if e < 0 { e = 0; }
                if e >= len { e = len - 1; }

                if s > e || s >= len {
                    return Frame::Array(vec![]);
                }

                let vals = zset.range_by_rank(s, e);
                Frame::Array(vals.into_iter().map(Frame::Bulk).collect())
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    async fn zrevrange(&self, key: String, start: i64, end: i64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                let len = zset.len() as i64;
                if len == 0 {
                    return Frame::Array(vec![]);
                }

                let mut s = if start < 0 { len + start } else { start };
                let mut e = if end < 0 { len + end } else { end };

                if s < 0 { s = 0; }
                if e < 0 { e = 0; }
                if e >= len { e = len - 1; }

                if s > e || s >= len {
                    return Frame::Array(vec![]);
                }

                let real_start = len - 1 - s;
                let real_end   = len - 1 - e;

                let vals = zset.range_by_rank(real_end, real_start);
                let rev = vals.into_iter().rev().collect::<Vec<_>>();

                Frame::Array(rev.into_iter().map(Frame::Bulk).collect())
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    async fn zcard(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => Frame::Integer(zset.len() as i64),   
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn zscore(&self, key: String, member: Vec<u8>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                match zset.get_score(&member) {
                    Some(score) => Frame::Bulk(score.to_string().into_bytes()),
                    None => Frame::Null,
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
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

    async fn zremrangebyscore(&self, key: String, min: f64, max: f64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::ZSet(zset)) => {
                let removed = zset.remove_range_by_score(min, max);
                Frame::Integer(removed as i64)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn zrank(&self, key: String, member: Vec<u8>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                match zset.rank(&member) {
                    Some(r) => Frame::Integer(r as i64),
                    None => Frame::Null,
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn zrevrank(&self, key: String, member: Vec<u8>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Null;
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                if let Some(r) = zset.rank(&member) {
                    let rev = zset.len() - 1 - r;
                    Frame::Integer(rev as i64)
                } else {
                    Frame::Null
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn zcount(&self, key: String, min: f64, max: f64) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::ZSet(zset)) => {
                let items = zset.range_by_score(min, max);
                Frame::Integer(items.len() as i64)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a keyholding the wrong kind of value".into()),
            None => Frame::Null,
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

    async fn hvals(&self, key: String) -> Frame {
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

    async fn sadd(&self, key: String, members: Vec<Vec<u8>>) -> Frame {
        self.check_and_purge(&key).await;

        let mut inner = self.inner.write().await;

        let entry = inner.entry(key).or_insert_with(|| Value::Set(HashSet::new()));

        match entry {
            Value::Set(set) => {
                let mut added = 0;
                for m in members {
                    if set.insert(m) {
                        added += 1;
                    }
                }
                Frame::Integer(added)
            }
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        }
    }

    async fn srem(&self, key: String, members: Vec<Vec<u8>>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let mut inner = self.inner.write().await;

        match inner.get_mut(&key) {
            Some(Value::Set(set)) => {
                let mut removed = 0;
                for m in members {
                    if set.remove(&m) {
                        removed += 1;
                    }
                }
                Frame::Integer(removed)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn smembers(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Array(vec![]);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Set(set)) => {
                let arr = set.iter()
                    .map(|v| Frame::Bulk(v.clone()))
                    .collect();
                Frame::Array(arr)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Array(vec![]),
        }
    }

    async fn sismember(&self, key: String, member: Vec<u8>) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Set(set)) => 
                Frame::Integer(if set.contains(&member) { 1 } else { 0 }),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn scard(&self, key: String) -> Frame {
        if self.check_and_purge(&key).await {
            return Frame::Integer(0);
        }

        let inner = self.inner.read().await;

        match inner.get(&key) {
            Some(Value::Set(set)) => Frame::Integer(set.len() as i64),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Integer(0),
        }
    }

    async fn sunion(&self, keys: Vec<String>) -> Frame {
        let inner = self.inner.read().await;

        let mut result = HashSet::new();

        for k in keys {
            if self.is_expired(&k).await {
                continue;
            }

            if let Some(Value::Set(set)) = inner.get(&k) {
                for v in set {
                    result.insert(v.clone());
                }
            }
        }

        let arr = result.into_iter().map(Frame::Bulk).collect();
        Frame::Array(arr)
    }

    async fn sinter(&self, keys: Vec<String>) -> Frame {
        let inner = self.inner.read().await;

        if keys.is_empty() {
            return Frame::Array(vec![]);
        }

        let mut iter = keys.iter();

        let mut base: Option<HashSet<Vec<u8>>> = None;

        for k in &keys {
            if self.is_expired(&k).await {
                continue;
            }

            if let Some(Value::Set(set)) = inner.get(k) {
                base = Some(set.clone());
                break;
            }
        }

        let Some(mut acc) = base else {
            return Frame::Array(vec![]);
        };

        for k in &keys {
            if let Some(Value::Set(set)) = inner.get(k) {
                acc = acc.intersection(set).cloned().collect();
            }
        }

        let arr = acc.into_iter().map(Frame::Bulk).collect();
        Frame::Array(arr)
    }

    async fn sdiff(&self, keys: Vec<String>) -> Frame {
        let inner = self.inner.read().await;

        if keys.is_empty() {
            return Frame::Array(vec![]);
        }

        let first = &keys[0];

        if self.is_expired(first).await {
            return Frame::Array(vec![]);
        }

        let base = match inner.get(first) {
            Some(Value::Set(set)) => set.clone(),
            _ => HashSet::new(),
        };

        let mut result = base;

        for k in keys.iter().skip(1) {
            if let Some(Value::Set(set)) = inner.get(k) {
                for m in set {
                    result.remove(m);
                }
            }
        }

        let arr = result.into_iter().map(Frame::Bulk).collect();
        Frame::Array(arr)
    }
}
