use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;

use claude_code_wechat_channel::{
    do_qr_login, get_updates, load_credentials, load_sync_buf, make_channel_notification,
    new_context_token_cache, parse_method_and_id, parse_tool_call, save_sync_buf,
    send_text_message, stdin_reader, stdio_log, stdio_log_error, stdout_writer,
    write_jsonrpc_message, AccountData, ContextTokenCache, BACKOFF_DELAY_MS, CHANNEL_NAME,
    CHANNEL_VERSION, MAX_CONSECUTIVE_FAILURES, RETRY_DELAY_MS,
};

#[derive(Clone)]
struct AppState {
    account: Arc<RwLock<Option<AccountData>>>,
    context_tokens: ContextTokenCache,
}

fn main() {
    if let Err(err) = run() {
        stdio_log_error(&format!("Fatal: {err}"));
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let account = load_or_login()?;
    let state = AppState {
        account: Arc::new(RwLock::new(Some(account))),
        context_tokens: new_context_token_cache(),
    };

    let writer = stdout_writer();
    let (tx, rx) = mpsc::channel::<String>();

    {
        let writer = writer.clone();
        thread::spawn(move || {
            while let Ok(message) = rx.recv() {
                if let Ok(mut out) = writer.lock() {
                    if let Err(err) = write_jsonrpc_message(&mut *out, &message) {
                        stdio_log_error(&format!("写入 JSON-RPC 消息失败: {err}"));
                        break;
                    }
                }
            }
        });
    }

    {
        let state = state.clone();
        let tx = tx.clone();
        thread::spawn(move || start_polling(state, tx));
    }

    stdio_log("MCP stdio 通道已连接");
    let mut reader = stdin_reader();
    while let Some(payload) =
        claude_code_wechat_channel::read_jsonrpc_message(&mut reader).map_err(|e| e.to_string())?
    {
        handle_rpc_message(&state, &tx, &payload)?;
    }
    Ok(())
}

fn load_or_login() -> Result<AccountData, String> {
    if let Some(account) = load_credentials()? {
        stdio_log(&format!("使用已保存账号: {}", account.account_id));
        return Ok(account);
    }
    stdio_log("未找到已保存的凭据，启动微信扫码登录...");
    do_qr_login(stdio_log)?.ok_or_else(|| "登录失败，退出。".to_string())
}

fn handle_rpc_message(
    state: &AppState,
    tx: &mpsc::Sender<String>,
    payload: &str,
) -> Result<(), String> {
    let (method, id) = parse_method_and_id(payload)?;
    let Some(method) = method else {
        return Ok(());
    };
    match method.as_str() {
        "initialize" => {
            if let Some(id) = id {
                let body = format!(
                    concat!(
                        "{{",
                        "\"jsonrpc\":\"2.0\",",
                        "\"id\":{},",
                        "\"result\":{{",
                        "\"protocolVersion\":\"2024-11-05\",",
                        "\"serverInfo\":{{\"name\":\"{}\",\"version\":\"{}\"}},",
                        "\"capabilities\":{{\"experimental\":{{\"claude/channel\":{{}}}},\"tools\":{{}}}},",
                        "\"instructions\":{}",
                        "}}",
                        "}}"
                    ),
                    id,
                    CHANNEL_NAME,
                    CHANNEL_VERSION,
                    claude_code_wechat_channel::json_string(
                        "Messages from WeChat users arrive as <channel source=\"wechat\" sender=\"...\" sender_id=\"...\">\nReply using the wechat_reply tool. You MUST pass the sender_id from the inbound tag.\nMessages are from real WeChat users via the WeChat ClawBot interface.\nRespond naturally in Chinese unless the user writes in another language.\nKeep replies concise — WeChat is a chat app, not an essay platform.\nStrip markdown formatting (WeChat doesn't render it). Use plain text."
                    )
                );
                let _ = tx.send(body);
            }
        }
        "notifications/initialized" => {}
        "tools/list" => {
            if let Some(id) = id {
                let body = format!(
                    concat!(
                        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"tools\":[{{",
                        "\"name\":\"wechat_reply\",",
                        "\"description\":\"Send a text reply back to the WeChat user\",",
                        "\"inputSchema\":{{",
                        "\"type\":\"object\",",
                        "\"properties\":{{",
                        "\"sender_id\":{{\"type\":\"string\",\"description\":\"The sender_id from the inbound <channel> tag (xxx@im.wechat format)\"}},",
                        "\"text\":{{\"type\":\"string\",\"description\":\"The plain-text message to send (no markdown)\"}}",
                        "}},",
                        "\"required\":[\"sender_id\",\"text\"]",
                        "}}",
                        "}}]}}}}"
                    ),
                    id
                );
                let _ = tx.send(body);
            }
        }
        "tools/call" => {
            if let Some(id) = id {
                let result = match handle_tool_call(state, payload) {
                    Ok(()) => "sent".to_string(),
                    Err(err) => format!("error: {err}"),
                };
                let body = format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":{}}}]}}}}",
                    id,
                    claude_code_wechat_channel::json_string(&result)
                );
                let _ = tx.send(body);
            }
        }
        "ping" => {
            if let Some(id) = id {
                let _ = tx.send(format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{}}}}",
                    id
                ));
            }
        }
        _ => {
            if let Some(id) = id {
                let body = format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":-32601,\"message\":{}}}}}",
                    id,
                    claude_code_wechat_channel::json_string(&format!("method not found: {method}"))
                );
                let _ = tx.send(body);
            }
        }
    }
    Ok(())
}

fn handle_tool_call(state: &AppState, payload: &str) -> Result<(), String> {
    let (name, sender_id, text) = parse_tool_call(payload)?;
    if name != "wechat_reply" {
        return Err(format!("unknown tool: {name}"));
    }
    let account = state
        .account
        .read()
        .map_err(|_| "failed to read account state".to_string())?
        .clone()
        .ok_or_else(|| "not logged in".to_string())?;
    let context_token = state
        .context_tokens
        .read()
        .map_err(|_| "failed to read context tokens".to_string())?
        .get(&sender_id)
        .cloned()
        .ok_or_else(|| {
            format!("no context_token for {sender_id}. The user may need to send a message first.")
        })?;
    send_text_message(&account, &sender_id, &text, &context_token)
}

fn start_polling(state: AppState, tx: mpsc::Sender<String>) {
    let account = match state.account.read() {
        Ok(guard) => match guard.clone() {
            Some(account) => account,
            None => {
                stdio_log_error("missing account");
                return;
            }
        },
        Err(_) => {
            stdio_log_error("failed to read account state");
            return;
        }
    };

    let mut get_updates_buf = load_sync_buf();
    if !get_updates_buf.is_empty() {
        stdio_log(&format!(
            "恢复上次同步状态 ({} bytes)",
            get_updates_buf.len()
        ));
    }
    stdio_log("开始监听微信消息...");

    let mut consecutive_failures = 0;
    loop {
        match get_updates(&account, &get_updates_buf) {
            Ok((new_buf, messages)) => {
                consecutive_failures = 0;
                if !new_buf.is_empty() {
                    get_updates_buf = new_buf;
                    save_sync_buf(&get_updates_buf);
                }
                for message in messages {
                    if let Some(token) = message.context_token.clone() {
                        if let Ok(mut guard) = state.context_tokens.write() {
                            guard.insert(message.sender_id.clone(), token);
                        }
                    }
                    stdio_log(&format!(
                        "收到消息: from={} text={}...",
                        message.sender_id,
                        truncate_for_log(&message.text)
                    ));
                    let _ = tx.send(make_channel_notification(&message.text, &message.sender_id));
                }
            }
            Err(err) => {
                consecutive_failures += 1;
                stdio_log_error(&format!("轮询异常: {err}"));
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    consecutive_failures = 0;
                    thread::sleep(Duration::from_millis(BACKOFF_DELAY_MS));
                } else {
                    thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
                }
            }
        }
    }
}

fn truncate_for_log(text: &str) -> String {
    let mut output = text.chars().take(50).collect::<String>();
    if text.chars().count() > 50 {
        output.push('…');
    }
    output
}
