use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, Duration};

use tokio::sync::RwLock;
use tokio::time;
use tokio::fs::File;
use tokio::io::BufReader;

use crate::command::Command;
use crate::connection::Connection;
use crate::resp::{Frame, encode_frame};
use crate::value::Value;
use crate::list::ListState;
use crate::skiplist::SkipList;
use crate::errors::RedisError;
use crate::aof::Aof;

#[derive(Debug)]
pub struct Db {
    inner: RwLock<HashMap<String, Value>>,
    ttl: RwLock<HashMap<String, Instant>>,
    aof: Option<Arc<Aof>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            ttl: RwLock::new(HashMap::new()),
            aof: None,
        }
    }

    pub fn new_with_aof(aof_path: Option<&str>) -> Self {
        let aof = aof_path.map(|p| Arc::new(Aof::new(p).unwrap()));

        Self {
            inner: RwLock::new(HashMap::new()),
            ttl: RwLock::new(HashMap::new()),
            aof,
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

    fn aof_write(&self, frame: &Frame) {
        if let Some(aof) = &self.aof {
            let encoded = encode_frame(frame);
            aof.append(&encoded);
        }
    }

    pub async fn load_aof(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(path).await?;
        let reader = BufReader::new(file);

        let mut conn = Connection::new_from_reader(reader);

        while let Ok(Some(frame)) = conn.read_frame().await {
            let cmd = Command::try_from(frame)?;
            self.apply(cmd).await;
        }

        Ok(())
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
        }
    }

    async fn get(&self, key: &str) -> Frame {
        let inner = self.inner.read().await;
        match inner.get(key) {
            Some(Value::String(v)) => Frame::Bulk(v.clone()),
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn set(&self, key: String, val: Vec<u8>) -> Frame {
        {
            let mut inner = self.inner.write().await;
            inner.insert(key.clone(), Value::String(val.clone()));
        }

        let frame = Frame::Array(vec![
            Frame::Bulk(b"SET".to_vec()),
            Frame::Bulk(key.clone().into_bytes()),
            Frame::Bulk(val.clone()),
        ]);
        self.aof_write(&frame);

        Frame::Simple("OK".into())
    }

    async fn lpush(&self, key: String, vals: Vec<Vec<u8>>) -> Frame {
        let mut inner = self.inner.write().await;
        let entry = inner.entry(key.clone())
            .or_insert_with(|| Value::List(ListState::new()));

        match entry {
            Value::List(list) => {
                for v in &vals {
                    list.data.push_front(v.clone());
                }
                list.notify.notify_one();

                let mut elements = vec![
                    Frame::Bulk(b"LPUSH".to_vec()),
                    Frame::Bulk(key.clone().into_bytes()),
                ];
                for v in vals.clone() {
                    elements.push(Frame::Bulk(v));
                }
                let frame = Frame::Array(elements);
                self.aof_write(&frame);

                Frame::Integer(list.data.len() as i64)
            }
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        }
    }

    async fn rpop(&self, key: &str) -> Frame {
        let mut inner = self.inner.write().await;

        let Some(value) = inner.get_mut(key) else {
            return Frame::Null;
        };

        if let Value::List(list) = value {
            if let Some(v) = list.data.pop_back() {
                let frame = Frame::Array(vec![
                    Frame::Bulk(b"RPOP".to_vec()),
                    Frame::Bulk(key.as_bytes().to_vec()),
                ]);
                self.aof_write(&frame);

                return Frame::Bulk(v);
            }
            Frame::Null
        } else {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
    }

    async fn brpop(&self, key: String, timeout_secs: usize) -> Frame {
        let timeout = Duration::from_secs(timeout_secs as u64);
        let deadline = time::Instant::now() + timeout;

        loop {
            let notify = {
                let mut inner = self.inner.write().await;
                let entry = inner.entry(key.clone())
                    .or_insert_with(|| Value::List(ListState::new()));

                match entry {
                    Value::List(list) => {
                        if let Some(v) = list.data.pop_back() {
                            let frame = Frame::Array(vec![
                                Frame::Bulk(b"RPOP".to_vec()),
                                Frame::Bulk(key.clone().into_bytes()),
                            ]);
                            self.aof_write(&frame);

                            return Frame::Array(vec![
                                Frame::Bulk(key.as_bytes().to_vec()),
                                Frame::Bulk(v),
                            ]);
                        }
                        list.notify.clone()
                    }
                    _ => {
                        return Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                        );
                    }
                }
            };

            let now = time::Instant::now();
            if now >= deadline {
                return Frame::Null;
            }

            let remaining = deadline - now;

            let notified = time::timeout(remaining, notify.notified()).await;
            if notified.is_err() {
                return Frame::Null;
            }
        }
    }

    async fn del(&self, key: &str) -> Frame {
        let mut inner = self.inner.write().await;
        let removed = inner.remove(key).is_some();

        let mut ttl = self.ttl.write().await;
        ttl.remove(key);
        
        let frame = Frame::Array(vec![
            Frame::Bulk(b"DEL".to_vec()),
            Frame::Bulk(key.as_bytes().to_vec()),
        ]);
        
        self.aof_write(&frame);

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
        let frame = Frame::Array(vec![
            Frame::Bulk(b"EXPIRE".to_vec()),
            Frame::Bulk(key.clone().into_bytes()),
            Frame::Bulk(secs.to_string().into_bytes()),
        ]);
        self.aof_write(&frame);
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
        let key_clone = key.clone();

        let mut inner = self.inner.write().await;
        let entry = inner
            .entry(key.clone())
            .or_insert_with(|| Value::ZSet(SkipList::new()));

        match entry {
            Value::ZSet(zset) => {
                zset.insert(score, member.clone());
                let frame = Frame::Array(vec![
                    Frame::Bulk(b"ZADD".to_vec()),
                    Frame::Bulk(key_clone.into_bytes()),
                    Frame::Bulk(score.to_string().into_bytes()),
                    Frame::Bulk(member.clone()),
                ]);
                self.aof_write(&frame);

                Frame::Integer(1)
            }
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        }
    }

    async fn zrange_by_score(&self, key: String, min: f64, max: f64) -> Frame {
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
        let key_clone = key.clone();

        let mut inner = self.inner.write().await;
        let Some(value) = inner.get_mut(&key) else {
            return Frame::Integer(0);
        };

        match value {
            Value::ZSet(zset) => {
                let removed = zset.remove_member(&member);

                if removed {
                    let frame = Frame::Array(vec![
                        Frame::Bulk(b"ZREM".to_vec()),
                        Frame::Bulk(key_clone.into_bytes()),
                        Frame::Bulk(member.clone()),
                    ]);
                    self.aof_write(&frame);
                }

                Frame::Integer(if removed { 1 } else { 0 })
            }
            _ => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        }
    }
}