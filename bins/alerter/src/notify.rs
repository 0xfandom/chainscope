//! Alert delivery.
//!
//! Behind a trait so the detectors never know how an alert is sent, and tests
//! can substitute a counting stub for the real Telegram call.

use async_trait::async_trait;

#[async_trait]
pub trait Notifier: Send + Sync {
    /// Deliver one alert message. Errors are the caller's to log; a failed send
    /// must not have claimed the dedupe row (see the engine).
    async fn send(&self, text: &str) -> anyhow::Result<()>;
}

/// Posts to the Telegram Bot API `sendMessage`.
pub struct Telegram {
    token: String,
    chat_id: String,
    http: reqwest::Client,
}

impl Telegram {
    pub fn new(token: String, chat_id: String) -> Self {
        Self {
            token,
            chat_id,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Notifier for Telegram {
    async fn send(&self, text: &str) -> anyhow::Result<()> {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let resp = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": true,
            }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("telegram sendMessage failed: {status} {body}");
        }
        Ok(())
    }
}
