use crate::errors::RedisError;
use crate::resp::Frame;

#[derive(Debug)]
pub enum Command {
    Ping,
    Get(String),
    Set(String, Vec<u8>),
    LPush(String, Vec<Vec<u8>>),
    RPop(String),
    BRPop(String, usize),
    Del(String),
    Expire(String, usize),
    Ttl(String),
    ZAdd(String, f64, Vec<u8>),
    ZRangeByScore(String, f64, f64),
    ZRem(String, Vec<u8>),

    // Hash commands
    HSet(String, String, Vec<u8>),
    HGet(String, String),
    HDel(String, Vec<String>),
    HGetAll(String),
    HMGet(String, Vec<String>),
    HExists(String, String),
    HLen(String),
    HKeys(String),
    HVals(String),
}

impl TryFrom<Frame> for Command {
    type Error = RedisError;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let arr = match frame {
            Frame::Array(a) => a,
            _ => return Err(RedisError::Other("expected array frame".into())),
        };

        if arr.is_empty() {
            return Err(RedisError::Other("empty command".into()));
        }

        let cmd_name = match &arr[0] {
            Frame::Bulk(b) => String::from_utf8_lossy(b).to_string().to_uppercase(),
            Frame::Simple(s) => s.to_uppercase(),
            _ => return Err(RedisError::Other("invalid command name".into())),
        };

        match cmd_name.as_str() {
            "PING" => Ok(Command::Ping),
            "GET" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'GET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::Get(key))
            }
            "SET" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let val = frame_to_bytes(&arr[2])?;
                Ok(Command::Set(key, val))
            }
            "LPUSH" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LPUSH'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut vals = Vec::new();
                for f in &arr[2..] {
                    vals.push(frame_to_bytes(f)?);
                }
                Ok(Command::LPush(key, vals))
            }
            "RPOP" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'RPOP'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::RPop(key))
            }
            "BRPOP" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'BRPOP'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let timeout_str = frame_to_string(&arr[2])?;
                let timeout: usize = timeout_str.parse().map_err(|_| {
                    RedisError::Other("ERR timeout must be integer".into())
                })?;
                Ok(Command::BRPop(key, timeout))
            }
            "DEL" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'DEL'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::Del(key))
            }
            "EXPIRE" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'EXPIRE'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let secs: usize = frame_to_string(&arr[2])?
                    .parse::<usize>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                Ok(Command::Expire(key, secs))
            }
            "TTL" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'TTL'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::Ttl(key))
            }
            "ZADD" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZADD'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let score: f64 = frame_to_string(&arr[2])?
                    .parse()
                    .map_err(|_| RedisError::Other("ERR score must be a float".into()))?;
                let member = frame_to_bytes(&arr[3])?;
                Ok(Command::ZAdd(key, score, member))
            }
            "ZRANGEBYSCORE" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other(
                        "ERR wrong number of arguments for 'ZRANGEBYSCORE'".into(),
                    ));
                }
                let key = frame_to_string(&arr[1])?;
                let min: f64 = frame_to_string(&arr[2])?
                    .parse()
                    .map_err(|_| RedisError::Other("ERR min must be a float".into()))?;
                let max: f64 = frame_to_string(&arr[3])?
                    .parse()
                    .map_err(|_| RedisError::Other("ERR max must be a float".into()))?;
                Ok(Command::ZRangeByScore(key, min, max))
            }
            "ZREM" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZREM'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let member = frame_to_bytes(&arr[2])?;
                Ok(Command::ZRem(key, member))
            }
            // Hash commands
            "HSET" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HSET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let field = frame_to_string(&arr[2])?;
                let value = frame_to_bytes(&arr[3])?;
                Ok(Command::HSet(key, field, value))
            }
            "HGET" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HGET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let field = frame_to_string(&arr[2])?;
                Ok(Command::HGet(key, field))
            }
            "HDEL" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HDEL'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut fields = Vec::new();
                for f in &arr[2..] {
                    fields.push(frame_to_string(f)?);
                }
                Ok(Command::HDel(key, fields))
            }
            "HGETALL" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HGETALL'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::HGetAll(key))
            }
            "HMGET" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HMGET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut fields = Vec::new();
                for f in &arr[2..] {
                    fields.push(frame_to_string(f)?);
                }
                Ok(Command::HMGet(key, fields))
            }
            "HEXISTS" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HEXISTS'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let field = frame_to_string(&arr[2])?;
                Ok(Command::HExists(key, field))
            }
            "HLEN" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HLEN'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::HLen(key))
            }
            "HKEYS" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HKEYS'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::HKeys(key))
            }
            "HVALS" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'HVALS'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::HVals(key))
            }
            _ => Err(RedisError::UnknownCommand),
        }
    }
}

fn frame_to_string(f: &Frame) -> Result<String, RedisError> {
    match f {
        Frame::Bulk(b) => Ok(String::from_utf8_lossy(b).to_string()),
        Frame::Simple(s) => Ok(s.clone()),
        _ => Err(RedisError::Other("expected bulk or simple string".into())),
    }
}

fn frame_to_bytes(f: &Frame) -> Result<Vec<u8>, RedisError> {
    match f {
        Frame::Bulk(b) => Ok(b.clone()),
        Frame::Simple(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(RedisError::Other("expected bulk or simple string".into())),
    }
}