use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time;

use crate::command::Command;
use crate::resp::Frame;
use crate::value::Value;
use crate::list::ListState;
use crate::errors::RedisError;

#[derive(Debug)]
pub struct Db {
    inner: RwLock<HashMap<String, Value>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
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
        }
    }

    async fn get(&self, key: &str) -> Frame {
        let inner = self.inner.read().await;
        match inner.get(key) {
            Some(Value::String(v)) => Frame::Bulk(v.clone()),
            Some(Value::List(_)) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Frame::Null,
        }
    }

    async fn set(&self, key: String, val: Vec<u8>) -> Frame {
        let mut inner = self.inner.write().await;
        inner.insert(key, Value::String(val));
        Frame::Simple("OK".into())
    }

    async fn lpush(&self, key: String, vals: Vec<Vec<u8>>) -> Frame {
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
        let timeout = Duration::from_secs(timeout_secs as u64);
        let deadline = time::Instant::now() + timeout;

        loop {
            let notify = {
                let mut inner = self.inner.write().await;
                let entry = inner
                    .entry(key.clone())
                    .or_insert_with(|| Value::List(ListState::new()));

                match entry {
                    Value::List(list) => {
                        if let Some(v) = list.data.pop_back() {
                            return Frame::Array(vec![
                                Frame::Bulk(key.as_bytes().to_vec()),
                                Frame::Bulk(v),
                            ]);
                        }
                        list.notify.clone()
                    }
                    _ => {
                        return Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
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
}