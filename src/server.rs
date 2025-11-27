use std::sync::Arc;
use tokio::net::TcpListener;

use crate::connection::Connection;
use crate::db::Db;
use crate::command::Command;
use crate::errors::RedisError;

pub async fn run(addr: &str, db: Arc<Db>) -> Result<(), RedisError> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        tokio::spawn(async move {
            let mut conn = Connection::new(socket);

            while let Ok(Some(frame)) = conn.read_frame().await {
                match Command::try_from(frame) {
                    Ok(cmd) => {
                        let response = db.apply(cmd).await;
                        if let Err(e) = conn.write_frame(&response).await {
                            eprintln!("error writing response: {:?}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("command parse error: {}", e);
                        let err_frame = crate::resp::Frame::Error(format!("ERR {}", e));
                        if let Err(e) = conn.write_frame(&err_frame).await {
                            eprintln!("error writing response: {:?}", e);
                            break;
                        }
                    }
                }
            }
        });
    }
}