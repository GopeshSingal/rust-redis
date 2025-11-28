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

use std::sync::Arc;
use db::Db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting rust-redis on 0.0.0.0:6379 ...");

    let db = Arc::new(Db::new());
    tokio::spawn(expiration::run(db.clone()));
    server::run("0.0.0.0:6379", db).await?;

    Ok(())
}