use anyhow::Result;
use claude_code_wechat_channel::{
    do_qr_login, handle_mcp_messages, load_credentials, log, log_error, start_polling, AccountData,
    SharedState, DEFAULT_BASE_URL,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        log_error(&format!("Fatal: {err}"));
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("claude-code-wechat-channel-rust/0.1.0")
        .build()?;
    let state = SharedState::new(client);

    let mcp_task = tokio::spawn(handle_mcp_messages(state.clone()));
    log("MCP 连接就绪");

    let account = match load_credentials() {
        Some(account) => {
            log(&format!("使用已保存账号: {}", account.account_id));
            account
        }
        None => {
            log("未找到已保存的凭据，启动微信扫码登录...");
            match do_qr_login(&state.client, DEFAULT_BASE_URL).await? {
                Some(account) => account,
                None => {
                    log_error("登录失败，退出。");
                    std::process::exit(1);
                }
            }
        }
    };

    {
        let mut guard = state.account.lock().await;
        *guard = Some(account.clone());
    }

    let polling_task = tokio::spawn(start_polling(state.clone(), account));

    tokio::select! {
        result = mcp_task => {
            result??;
        }
        result = polling_task => {
            result??;
        }
    }

    Ok(())
}
