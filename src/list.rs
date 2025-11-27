use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug)]
pub struct ListState {
    pub data: VecDeque<Vec<u8>>,
    pub notify: Arc<Notify>,
}

impl ListState {
    pub fn new() -> Self {
        Self {
            data: VecDeque::new(),
            notify: Arc::new(Notify::new()),
        }
    }
}