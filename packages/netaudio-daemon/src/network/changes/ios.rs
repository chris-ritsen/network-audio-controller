use anyhow::Result;
use tokio::sync::Notify;

static CHANGED: Notify = Notify::const_new();

pub fn announce() {
    CHANGED.notify_one();
}

pub struct Changes;

impl Changes {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    pub async fn next(&mut self) -> Result<()> {
        CHANGED.notified().await;
        Ok(())
    }
}
