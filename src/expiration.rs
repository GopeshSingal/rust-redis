use std::sync::Arc;
use std::time::Instant;

use tokio::time::{self, Duration};

use crate::db::Db;

pub async fn run(db: Arc<Db>) {
    let mut interval = time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        cleanup(&db).await;
    }
}

async fn cleanup(db: &Db) {
    let mut expired_keys = vec![];
    
    {
        let ttl = db.get_ttl().await;
        let now = Instant::now();
        for (k, exp) in ttl.iter() {
            if now >= *exp {
                expired_keys.push(k.clone());
            }
        }
    }

    if expired_keys.is_empty() {
        return;
    }

    let mut inner = db.get_inner_mut().await;
    let mut ttl_mut = db.get_ttl_mut().await;

    for key in expired_keys{
        inner.remove(&key);
        ttl_mut.remove(&key);
    }
}