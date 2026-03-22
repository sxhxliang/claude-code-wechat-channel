use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const CHANNEL_NAME: &str = "wechat";
pub const CHANNEL_VERSION: &str = "0.1.0";
pub const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
pub const BOT_TYPE: &str = "3";
pub const LONG_POLL_TIMEOUT_MS: u64 = 35_000;
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;
pub const BACKOFF_DELAY_MS: u64 = 30_000;
pub const RETRY_DELAY_MS: u64 = 2_000;
pub const MSG_TYPE_USER: i64 = 1;

#[derive(Debug, Clone)]
pub struct AccountData {
    pub token: String,
    pub base_url: String,
    pub account_id: String,
    pub user_id: Option<String>,
    pub saved_at: String,
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub sender_id: String,
    pub text: String,
    pub context_token: Option<String>,
}

pub type ContextTokenCache = Arc<RwLock<HashMap<String, String>>>;

pub fn new_context_token_cache() -> ContextTokenCache {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn stdio_log(message: &str) {
    eprintln!("[wechat-channel] {message}");
}

pub fn stdio_log_error(message: &str) {
    eprintln!("[wechat-channel] ERROR: {message}");
}

pub fn credentials_dir() -> PathBuf {
    home_dir().join(".claude/channels/wechat")
}

pub fn credentials_file() -> PathBuf {
    credentials_dir().join("account.json")
}

pub fn sync_buf_file() -> PathBuf {
    credentials_dir().join("sync_buf.txt")
}

pub fn home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("~"))
}

pub fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn save_credentials(account: &AccountData) -> Result<(), String> {
    let path = credentials_file();
    ensure_parent_dir(&path).map_err(|e| e.to_string())?;
    let json = [
        "{
"
        .to_string(),
        format!(
            "  \"token\": {},
",
            json_string(&account.token)
        ),
        format!(
            "  \"baseUrl\": {},
",
            json_string(&account.base_url)
        ),
        format!(
            "  \"accountId\": {},
",
            json_string(&account.account_id)
        ),
        format!(
            "  \"userId\": {},
",
            account
                .user_id
                .as_ref()
                .map(|value| json_string(value))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "  \"savedAt\": {}
",
            json_string(&account.saved_at)
        ),
        "}
"
        .to_string(),
    ]
    .join("");
    fs::write(&path, json).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn load_credentials() -> Result<Option<AccountData>, String> {
    let path = credentials_file();
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let python = r#"
import json, sys
obj=json.loads(sys.stdin.read())
vals=[obj.get('token',''), obj.get('baseUrl',''), obj.get('accountId',''), obj.get('userId') or '', obj.get('savedAt','')]
for item in vals:
    print(item)
"#;
    let output = run_python_with_input(python, &data)?;
    let mut lines = output.lines();
    let token = lines.next().unwrap_or_default().to_string();
    if token.is_empty() {
        return Ok(None);
    }
    Ok(Some(AccountData {
        token,
        base_url: lines.next().unwrap_or(DEFAULT_BASE_URL).to_string(),
        account_id: lines.next().unwrap_or_default().to_string(),
        user_id: match lines.next().unwrap_or_default() {
            "" => None,
            value => Some(value.to_string()),
        },
        saved_at: lines.next().unwrap_or_default().to_string(),
    }))
}

pub fn load_sync_buf() -> String {
    fs::read_to_string(sync_buf_file()).unwrap_or_default()
}

pub fn save_sync_buf(buf: &str) {
    let path = sync_buf_file();
    let _ = ensure_parent_dir(&path);
    let _ = fs::write(path, buf);
}

pub fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn now_iso() -> String {
    let python = "import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z'))";
    run_python_with_input(python, "")
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
        .trim()
        .to_string()
}

pub fn random_wechat_uin() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let value = (nanos as u32).to_string();
    base64_simple(value.as_bytes())
}

fn base64_simple(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i];
        let b1 = *input.get(i + 1).unwrap_or(&0);
        let b2 = *input.get(i + 2).unwrap_or(&0);
        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | b2 as u32;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if i + 1 < input.len() {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < input.len() {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

pub fn api_post_json(
    base_url: &str,
    endpoint: &str,
    body: &str,
    token: Option<&str>,
    timeout_ms: u64,
) -> Result<String, String> {
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    );
    let mut command = Command::new("curl");
    command
        .arg("-sS")
        .arg("-X")
        .arg("POST")
        .arg(url)
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-H")
        .arg("AuthorizationType: ilink_bot_token")
        .arg("-H")
        .arg(format!("X-WECHAT-UIN: {}", random_wechat_uin()))
        .arg("--max-time")
        .arg(format!("{}", timeout_ms / 1000 + 1))
        .arg("--data-binary")
        .arg(body);
    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        command
            .arg("-H")
            .arg(format!("Authorization: Bearer {}", token.trim()));
    }
    let output = command.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn fetch_qrcode(base_url: &str) -> Result<(String, String), String> {
    let url = format!(
        "{}/ilink/bot/get_bot_qrcode?bot_type={}",
        base_url.trim_end_matches('/'),
        BOT_TYPE
    );
    let output = Command::new("curl")
        .arg("-sS")
        .arg(url)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    let raw = String::from_utf8_lossy(&output.stdout).to_string();
    let python = r#"
import json, sys
obj=json.loads(sys.stdin.read())
print(obj.get('qrcode',''))
print(obj.get('qrcode_img_content',''))
"#;
    let text = run_python_with_input(python, &raw)?;
    let mut lines = text.lines();
    Ok((
        lines.next().unwrap_or_default().to_string(),
        lines.next().unwrap_or_default().to_string(),
    ))
}

pub fn poll_qr_status(base_url: &str, qrcode: &str) -> Result<(String, Vec<String>), String> {
    let url = format!(
        "{}/ilink/bot/get_qrcode_status?qrcode={}",
        base_url.trim_end_matches('/'),
        percent_encode(qrcode)
    );
    let output = Command::new("curl")
        .arg("-sS")
        .arg("-H")
        .arg("iLink-App-ClientVersion: 1")
        .arg("--max-time")
        .arg("36")
        .arg(url)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if err.contains("timed out") {
            return Ok(("wait".to_string(), vec![]));
        }
        return Err(err);
    }
    let raw = String::from_utf8_lossy(&output.stdout).to_string();
    let python = r#"
import json, sys
obj=json.loads(sys.stdin.read())
print(obj.get('status',''))
print(obj.get('bot_token') or '')
print(obj.get('ilink_bot_id') or '')
print(obj.get('baseurl') or '')
print(obj.get('ilink_user_id') or '')
"#;
    let text = run_python_with_input(python, &raw)?;
    let mut lines = text.lines();
    let status = lines.next().unwrap_or_default().to_string();
    Ok((status, lines.map(|line| line.to_string()).collect()))
}

pub fn do_qr_login<F: FnMut(&str)>(mut log: F) -> Result<Option<AccountData>, String> {
    log("正在获取微信登录二维码...");
    let (qrcode, qr_image) = fetch_qrcode(DEFAULT_BASE_URL)?;
    log("");
    log("请使用微信扫描以下二维码链接：");
    log(&qr_image);
    log("等待扫码...");

    let start = SystemTime::now();
    let mut scanned = false;
    loop {
        if SystemTime::now()
            .duration_since(start)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
            > 480
        {
            log("登录超时");
            return Ok(None);
        }
        let (status, values) = poll_qr_status(DEFAULT_BASE_URL, &qrcode)?;
        match status.as_str() {
            "wait" => {}
            "scaned" => {
                if !scanned {
                    log("👀 已扫码，请在微信中确认...");
                    scanned = true;
                }
            }
            "expired" => {
                log("二维码已过期，请重新启动。");
                return Ok(None);
            }
            "confirmed" => {
                let token = values.first().cloned().unwrap_or_default();
                let account_id = values.get(1).cloned().unwrap_or_default();
                if token.is_empty() || account_id.is_empty() {
                    return Err("登录确认但未返回完整 bot 信息".to_string());
                }
                let account = AccountData {
                    token,
                    base_url: values
                        .get(2)
                        .cloned()
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
                    account_id,
                    user_id: values.get(3).cloned().filter(|value| !value.is_empty()),
                    saved_at: now_iso(),
                };
                save_credentials(&account)?;
                log("✅ 微信连接成功！");
                return Ok(Some(account));
            }
            _ => {}
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn send_text_message(
    account: &AccountData,
    sender_id: &str,
    text: &str,
    context_token: &str,
) -> Result<(), String> {
    let client_id = format!(
        "claude-code-wechat:{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let body = format!(
        r#"{{"msg":{{"from_user_id":"","to_user_id":{},"client_id":{},"message_type":2,"message_state":2,"item_list":[{{"type":1,"text_item":{{"text":{}}}}}],"context_token":{}}},"base_info":{{"channel_version":{}}}}}"#,
        json_string(sender_id),
        json_string(&client_id),
        json_string(text),
        json_string(context_token),
        json_string(CHANNEL_VERSION),
    );
    api_post_json(
        &account.base_url,
        "ilink/bot/sendmessage",
        &body,
        Some(&account.token),
        15_000,
    )?;
    Ok(())
}

pub fn get_updates(
    account: &AccountData,
    get_updates_buf: &str,
) -> Result<(String, Vec<IncomingMessage>), String> {
    let body = format!(
        "{{\"get_updates_buf\":{},\"base_info\":{{\"channel_version\":{}}}}}",
        json_string(get_updates_buf),
        json_string(CHANNEL_VERSION)
    );
    let raw = api_post_json(
        &account.base_url,
        "ilink/bot/getupdates",
        &body,
        Some(&account.token),
        LONG_POLL_TIMEOUT_MS,
    )?;
    let python = r#"
import json, sys
obj=json.loads(sys.stdin.read())
ret=obj.get('ret',0)
err=obj.get('errcode',0)
errmsg=obj.get('errmsg') or ''
print(obj.get('get_updates_buf') or '')
print(f'__STATUS__\t{ret}\t{err}\t{errmsg}')
for msg in obj.get('msgs') or []:
    if msg.get('message_type') != 1:
        continue
    text=''
    for item in msg.get('item_list') or []:
        if item.get('type') == 1 and (item.get('text_item') or {}).get('text'):
            text=(item.get('text_item') or {}).get('text') or ''
            ref=item.get('ref_msg') or {}
            title=ref.get('title') or ''
            if title:
                text=f'[引用: {title}]\n{text}'
            break
        if item.get('type') == 3 and (item.get('voice_item') or {}).get('text'):
            text=(item.get('voice_item') or {}).get('text') or ''
            break
    if not text:
        continue
    sender=msg.get('from_user_id') or 'unknown'
    token=msg.get('context_token') or ''
    print('__MSG__\t' + sender.replace('\t',' ') + '\t' + token.replace('\t',' ') + '\t' + text.replace('\n','\\n').replace('\t',' '))
"#;
    let parsed = run_python_with_input(python, &raw)?;
    let mut lines = parsed.lines();
    let new_buf = lines.next().unwrap_or_default().to_string();
    let status = lines.next().unwrap_or_default();
    let mut parts = status.splitn(4, '\t');
    let marker = parts.next().unwrap_or_default();
    if marker != "__STATUS__" {
        return Err("invalid getupdates response".to_string());
    }
    let ret = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
    let errcode = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
    let errmsg = parts.next().unwrap_or_default();
    if ret != 0 || errcode != 0 {
        return Err(format!(
            "getUpdates 失败: ret={ret} errcode={errcode} errmsg={errmsg}"
        ));
    }
    let mut messages = Vec::new();
    for line in lines {
        let mut fields = line.splitn(4, '\t');
        if fields.next().unwrap_or_default() != "__MSG__" {
            continue;
        }
        let sender_id = fields.next().unwrap_or("unknown").to_string();
        let context_token = match fields.next().unwrap_or_default() {
            "" => None,
            value => Some(value.to_string()),
        };
        let text = fields.next().unwrap_or_default().replace("\\n", "\n");
        messages.push(IncomingMessage {
            sender_id,
            text,
            context_token,
        });
    }
    Ok((new_buf, messages))
}

pub fn percent_encode(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

pub fn run_python_with_input(script: &str, input: &str) -> Result<String, String> {
    let mut child = Command::new("python3")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(input.as_bytes())
            .map_err(|e| e.to_string())?;
    }
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn read_jsonrpc_message<R: BufRead>(reader: &mut R) -> io::Result<Option<String>> {
    let mut content_length = None;
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case("Content-Length") {
                content_length = value.trim().parse::<usize>().ok();
            }
        }
    }
    let Some(length) = content_length else {
        return Ok(None);
    };
    let mut buffer = vec![0_u8; length];
    reader.read_exact(&mut buffer)?;
    Ok(Some(String::from_utf8_lossy(&buffer).to_string()))
}

pub fn write_jsonrpc_message<W: Write>(writer: &mut W, body: &str) -> io::Result<()> {
    write!(writer, "Content-Length: {}\r\n\r\n{}", body.len(), body)?;
    writer.flush()
}

pub fn make_channel_notification(text: &str, sender_id: &str) -> String {
    format!(
        concat!(
            "{{",
            "\"jsonrpc\":\"2.0\",",
            "\"method\":\"notifications/claude/channel\",",
            "\"params\":{{",
            "\"content\":{},",
            "\"meta\":{{\"sender\":{},\"sender_id\":{}}}",
            "}}",
            "}}"
        ),
        json_string(text),
        json_string(sender_id.split('@').next().unwrap_or(sender_id)),
        json_string(sender_id),
    )
}

pub fn extract_rpc_field(payload: &str, field: &str) -> Option<String> {
    let script = format!(
        r#"import json, sys
obj=json.loads(sys.stdin.read())
value=obj.get({field:?})
if value is None:
    sys.exit(0)
if isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=False))
else:
    print(value)
"#,
    );
    run_python_with_input(&script, payload)
        .ok()
        .map(|value| value.trim_end().to_string())
        .filter(|value| !value.is_empty())
}

pub fn parse_tool_call(payload: &str) -> Result<(String, String, String), String> {
    let script = r#"
import json, sys
obj=json.loads(sys.stdin.read())
params=obj.get('params') or {}
args=params.get('arguments') or {}
print(params.get('name') or '')
print(args.get('sender_id') or '')
print(args.get('text') or '')
"#;
    let output = run_python_with_input(script, payload)?;
    let mut lines = output.lines();
    Ok((
        lines.next().unwrap_or_default().to_string(),
        lines.next().unwrap_or_default().to_string(),
        lines.next().unwrap_or_default().to_string(),
    ))
}

pub fn parse_method_and_id(payload: &str) -> Result<(Option<String>, Option<String>), String> {
    let script = r#"
import json, sys
obj=json.loads(sys.stdin.read())
method=obj.get('method')
id_value=obj.get('id')
if method is not None:
    print(method)
else:
    print('')
if id_value is None:
    print('')
else:
    print(json.dumps(id_value, ensure_ascii=False))
"#;
    let output = run_python_with_input(script, payload)?;
    let mut lines = output.lines();
    let method = lines.next().unwrap_or_default().to_string();
    let id = lines.next().unwrap_or_default().to_string();
    Ok((
        if method.is_empty() {
            None
        } else {
            Some(method)
        },
        if id.is_empty() { None } else { Some(id) },
    ))
}

pub fn stdout_writer() -> Arc<Mutex<io::Stdout>> {
    Arc::new(Mutex::new(io::stdout()))
}

pub fn stdin_reader() -> BufReader<io::Stdin> {
    BufReader::new(io::stdin())
}
