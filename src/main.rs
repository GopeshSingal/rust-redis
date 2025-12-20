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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting rust-redis on 0.0.0.0:6379 ...");

    let db = Arc::new(Db::new());

    let aof = Arc::new(aof::Aof::open("appendonly.aof").await?);

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
    server::run("0.0.0.0:6379", db, aof).await?;

    Ok(())
}
