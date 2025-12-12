use crate::errors::RedisError;
use crate::resp::Frame;

#[derive(Debug)]
pub enum Command {
    Ping,
    
    // Keyspace commands
    Expire(String, usize),
    Ttl(String),

    // String commands
    Get(String),
    Set(String, Vec<u8>),
    Del(String),
    Append(String, Vec<u8>),
    StrLen(String),
    GetSet(String, Vec<u8>),
    Incr(String),
    IncrBy(String, i64),
    MSet(Vec<(String, Vec<u8>)>),
    MGet(Vec<String>),

    // List Commands
    LPush(String, Vec<Vec<u8>>),
    LPop(String),
    RPush(String, Vec<Vec<u8>>),
    RPop(String),
    LLen(String),
    LRange(String, i64, i64),
    LIndex(String, i64),
    LSet(String, i64, Vec<u8>),
    LTrim(String, i64, i64),
    BRPop(String, usize),

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

    // Set commands
    SAdd(String, Vec<Vec<u8>>),
    SRem(String, Vec<Vec<u8>>),
    SMembers(String),
    SIsMember(String, Vec<u8>),
    SCard(String),
    SUnion(Vec<String>),
    SInter(Vec<String>),
    SDiff(Vec<String>),
    
    // Sorted Set Commands
    ZAdd(String, f64, Vec<u8>),
    ZRem(String, Vec<u8>),
    ZRange(String, i64, i64),
    ZRevRange(String, i64, i64),
    ZCard(String),
    ZScore(String, Vec<u8>),
    ZRangeByScore(String, f64, f64),
    ZRemRangeByScore(String, f64, f64),
    ZRank(String, Vec<u8>),
    ZRevRank(String, Vec<u8>),
    ZCount(String, f64, f64),
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
            
            // Keyspace commands
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

            // String commands
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
            "DEL" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'DEL'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::Del(key))
            }
            "APPEND" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'APPEND'".into()));
                }
                Ok(Command::Append(
                    frame_to_string(&arr[1])?,
                    frame_to_bytes(&arr[2])?,
                ))
            }
            "STRLEN" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'STRLEN'".into()));
                }
                Ok(Command::StrLen(frame_to_string(&arr[1])?))
            }
            "GETSET" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'GETSET'".into()));
                }
                Ok(Command::GetSet(
                    frame_to_string(&arr[1])?,
                    frame_to_bytes(&arr[2])?,
                ))
            }
            "INCR" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'INCR'".into()));
                }
                Ok(Command::Incr(frame_to_string(&arr[1])?))
            }
            "INCRBY" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'INCRBY'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let amt_str = frame_to_string(&arr[2])?;
                let amt = amt_str.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                Ok(Command::IncrBy(key, amt))
            }
            "MSET" => {
                if arr.len() < 3 || arr.len() % 2 == 0 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'MSET'".into()));
                }
                let mut kvs = Vec::new();
                for pair in arr[1..].chunks(2) {
                    let key = frame_to_string(&pair[0])?;
                    let val = frame_to_bytes(&pair[1])?;
                    kvs.push((key, val));
                }
                Ok(Command::MSet(kvs))
            }
            "MGET" => {
                if arr.len() < 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'MGET'".into()));
                }
                let mut keys = Vec::new();
                for k in &arr[1..] {
                    keys.push(frame_to_string(k)?);
                }
                Ok(Command::MGet(keys))
            }

            // List commands
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
            "LPOP" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LPOP'".into()));
                }
                Ok(Command::LPop(frame_to_string(&arr[1])?))
            }
            "RPUSH" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'RPUSH'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut vals = Vec::new();
                for f in &arr[2..] {
                    vals.push(frame_to_bytes(f)?);
                }
                Ok(Command::RPush(key, vals))
            }
            "RPOP" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'RPOP'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                Ok(Command::RPop(key))
            }
            "LLEN" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LLEN'".into()));
                }
                Ok(Command::LLen(frame_to_string(&arr[1])?))
            }
            "LRANGE" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LRANGE'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let start = frame_to_string(&arr[2])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                let stop = frame_to_string(&arr[3])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                Ok(Command::LRange(key, start, stop))
            }
            "LINDEX" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LINDEX'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let index = frame_to_string(&arr[2])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                Ok(Command::LIndex(key, index))
            }
            "LSET" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LSET'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let index = frame_to_string(&arr[2])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                let value = frame_to_bytes(&arr[3])?;
                Ok(Command::LSet(key, index, value))
            }
            "LTRIM" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'LTRIM'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let start = frame_to_string(&arr[2])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                let stop = frame_to_string(&arr[3])?.parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR value is not an integer".into()))?;
                Ok(Command::LTrim(key, start, stop))
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

            // Set commands
            "SADD" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SADD'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut members = Vec::new();
                for f in &arr[2..] {
                    members.push(frame_to_bytes(f)?);
                }
                Ok(Command::SAdd(key, members))
            }
            "SREM" => {
                if arr.len() < 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SREM'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let mut members = Vec::new();
                for f in &arr[2..] {
                    members.push(frame_to_bytes(f)?);
                }
                Ok(Command::SRem(key, members))
            }
            "SMEMBERS" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SMEMBERS'".into()));
                }
                Ok(Command::SMembers(frame_to_string(&arr[1])?))
            }
            "SISMEMBER" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SISMEMBER'".into()));
                }
                Ok(Command::SIsMember(frame_to_string(&arr[1])?, frame_to_bytes(&arr[2])?))
            }
            "SCARD" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SCARD'".into()));
                }
                Ok(Command::SCard(frame_to_string(&arr[1])?))
            }
            "SUNION" => {
                if arr.len() < 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SUNION'".into()));
                }
                let keys = arr[1..].iter()
                    .map(|f| frame_to_string(f))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SUnion(keys))
            }
            "SINTER" => {
                if arr.len() < 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SINTER'".into()));
                }
                let keys = arr[1..].iter()
                    .map(|f| frame_to_string(f))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SInter(keys))
            }
            "SDIFF" => {
                if arr.len() < 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'SDIFF'".into()));
                }
                let keys = arr[1..].iter()
                    .map(|f| frame_to_string(f))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SDiff(keys))
            }
            
            // Sorted Set commands
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
            "ZREM" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZREM'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let member = frame_to_bytes(&arr[2])?;
                Ok(Command::ZRem(key, member))
            }
            "ZRANGE" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other(
                        "ERR wrong number of arguments for 'ZRANGE'".into(),
                    ));
                }
                let key = frame_to_string(&arr[1])?;
                let start = frame_to_string(&arr[2])?
                    .parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR start must be an integer".into()))?;
                let end = frame_to_string(&arr[3])?
                    .parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR stop must be an integer".into()))?;
                Ok(Command::ZRange(key, start, end))
            }
            "ZREVRANGE" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other(
                        "ERR wrong number of arguments for 'ZREVRANGE'".into(),
                    ));
                }
                let key = frame_to_string(&arr[1])?;
                let start = frame_to_string(&arr[2])?
                    .parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR start must be an integer".into()))?;
                let end = frame_to_string(&arr[3])?
                    .parse::<i64>()
                    .map_err(|_| RedisError::Other("ERR start must be an integer".into()))?;
                Ok(Command::ZRevRange(key, start, end))
            }
            "ZCARD" => {
                if arr.len() != 2 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZCARD'".into()));
                }
                Ok(Command::ZCard(frame_to_string(&arr[1])?))
            }
            "ZSCORE" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other(
                            "ERR wrong number of arguments for 'ZSCORE'".into()
                    ));
                }
                let key = frame_to_string(&arr[1])?;
                let member = frame_to_bytes(&arr[2])?;
                Ok(Command::ZScore(key, member))
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
            "ZREMRANGEBYSCORE" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other(
                        "ERR wrong number of arguments for 'ZREMRANGEBYSCORE'".into(),
                    ));
                }
                let key = frame_to_string(&arr[1])?;
                let min: f64 = frame_to_string(&arr[2])?
                    .parse()
                    .map_err(|_| RedisError::Other("ERR min must be a float".into()))?;
                let max: f64 = frame_to_string(&arr[3])?
                    .parse()
                    .map_err(|_| RedisError::Other("ERR max must be a float".into()))?;
                Ok(Command::ZRemRangeByScore(key, min, max))
            }
            "ZRANK" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZRANK'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let member = frame_to_bytes(&arr[2])?;
                Ok(Command::ZRank(key, member))
            }
            "ZREVRANK" => {
                if arr.len() != 3 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZREVRANK'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let member = frame_to_bytes(&arr[2])?;
                Ok(Command::ZRevRank(key, member))
            }
            "ZCOUNT" => {
                if arr.len() != 4 {
                    return Err(RedisError::Other("ERR wrong number of arguments for 'ZCOUNT'".into()));
                }
                let key = frame_to_string(&arr[1])?;
                let min = frame_to_string(&arr[2])?
                    .parse::<f64>()
                    .map_err(|_| RedisError::Other("ERR start must be a float".into()))?;
                let max = frame_to_string(&arr[3])?
                    .parse::<f64>()
                    .map_err(|_| RedisError::Other("ERR start must be a float".into()))?;
                Ok(Command::ZCount(key, min, max))
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
