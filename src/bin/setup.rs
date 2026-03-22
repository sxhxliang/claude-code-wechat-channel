use anyhow::Result;
use chrono::Utc;
use claude_code_wechat_channel::{
    credentials_file, do_qr_login, load_credentials, prompt_yes_no, save_credentials,
    DEFAULT_BASE_URL,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("错误: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    if let Some(existing) = load_credentials() {
        println!("已有保存的账号: {}", existing.account_id);
        println!("保存时间: {}", existing.saved_at);
        println!();
        if !prompt_yes_no("是否重新登录？(y/N) ")? {
            println!("保持现有凭据，退出。");
            return Ok(());
        }
    }

    let client = reqwest::Client::builder()
        .user_agent("claude-code-wechat-channel-rust/0.1.0")
        .build()?;
    println!("正在获取微信登录二维码...\n");
    if let Some(mut account) = do_qr_login(&client, DEFAULT_BASE_URL).await? {
        account.saved_at = Utc::now().to_rfc3339();
        save_credentials(&account)?;
        println!("\n✅ 微信连接成功！");
        println!("   账号 ID: {}", account.account_id);
        println!("   用户 ID: {}", account.user_id.as_deref().unwrap_or(""));
        println!("   凭据保存至: {}", credentials_file().display());
        println!();
        println!("现在可以启动 Claude Code 通道：");
        println!("  cargo run --bin wechat-channel");
        return Ok(());
    }

    anyhow::bail!("登录超时或二维码已过期")
}
