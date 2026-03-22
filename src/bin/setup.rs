use std::io::{self, Write};

use claude_code_wechat_channel::{credentials_file, do_qr_login, load_credentials, stdio_log};

fn main() {
    if let Err(err) = run() {
        eprintln!("错误: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    if let Some(existing) = load_credentials()? {
        println!("已有保存的账号: {}", existing.account_id);
        println!("保存时间: {}", existing.saved_at);
        println!();
        print!("是否重新登录？(y/N) ");
        io::stdout().flush().map_err(|e| e.to_string())?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .map_err(|e| e.to_string())?;
        if answer.trim().to_lowercase() != "y" {
            println!("保持现有凭据，退出。");
            return Ok(());
        }
    }

    match do_qr_login(|message| println!("{message}"))? {
        Some(account) => {
            println!("   账号 ID: {}", account.account_id);
            println!(
                "   用户 ID: {}",
                account.user_id.as_deref().unwrap_or("<unknown>")
            );
            println!("   凭据保存至: {}", credentials_file().display());
            println!();
            println!("现在可以启动 Claude Code 通道：");
            println!("  cargo run --bin wechat-channel");
        }
        None => {
            stdio_log("登录未完成。");
            std::process::exit(1);
        }
    }
    Ok(())
}
