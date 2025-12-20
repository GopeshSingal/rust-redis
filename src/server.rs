use std::sync::Arc;
use tokio::net::TcpListener;

use crate::connection::Connection;
use crate::db::Db;
use crate::command::Command;
use crate::errors::RedisError;
use crate::aof::Aof;

pub async fn run(addr: &str, db: Arc<Db>, aof: Arc<Aof>) -> Result<(), RedisError> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        let aof = aof.clone();

        tokio::spawn(async move {
            let mut conn = Connection::new(socket);

            while let Ok(Some(frame)) = conn.read_frame().await {
                let original_frame = frame.clone();

                match Command::try_from(frame) {
                    Ok(cmd) => {
                        let should_log = cmd.is_write_for_aof();
                        let response = db.apply(cmd).await;
                        if should_log && !matches!(response, crate::resp::Frame::Error(_)) {
                            if let Err(e) = aof.append_frame(&original_frame).await {
                                eprintln!("AOF append error: {:?}", e);
                            }
                        }
                        
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
