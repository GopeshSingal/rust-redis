mod server;
mod connection;
mod resp;
mod command;
mod errors;
mod db;
mod value;
mod list;
mod expiration;
mod skiplist;
mod aof;

use std::sync::Arc;
use db::Db;

#[derive(Debug)]
struct Config {
    addr: String,
    aof_path: String,
    aof_fsync: aof::AofFsync,
}

impl Config {
    fn from_args() -> anyhow::Result<Self> {
        let mut addr = "0.0.0.0:6379".to_string();
        let mut aof_path = "appendonly.aof".to_string();
        let mut aof_fsync = aof::AofFsync::EverySec;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    addr = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--addr requires a value"))?;
                }
                "--aof" => {
                    aof_path = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--aof requires a value"))?;
                }
                "--aof-fsync" => {
                    let v = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--aof-fsync requires a value"))?;
                    aof_fsync = aof::AofFsync::parse(&v)
                        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                }
                other => {
                    return Err(anyhow::anyhow!("unknown argument: {}", other));
                }
            }
        }

        Ok(Self {
            addr,
            aof_path,
            aof_fsync,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_args()?;

    println!(
        "Starting rust-redis on {} (AOF: {}, fsync: {:?}) ...",
        cfg.addr, cfg.aof_path, cfg.aof_fsync
    );

    let db = Arc::new(Db::new());
    let aof = aof::Aof::open(&cfg.aof_path, cfg.aof_fsync).await?;

    match tokio::fs::read(aof.path()).await {
        Ok(bytes) => {
            let frames = aof::parse_frames_from_bytes(&bytes)?;
            for frame in frames {
                if let Ok(cmd) = crate::command::Command::try_from(frame) {
                    let _ = db.apply(cmd).await;
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }

    tokio::spawn(expiration::run(db.clone()));
    server::run(&cfg.addr, db, aof).await?;

    Ok(())
}
