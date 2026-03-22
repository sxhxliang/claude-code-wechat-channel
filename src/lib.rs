use aes::cipher::{generic_array::GenericArray, BlockDecrypt, KeyInit};
use aes::Aes128;
use anyhow::{anyhow, Context, Result};
use base64::Engine;
use chrono::Utc;
use rand::RngCore;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader as AsyncBufReader};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

pub const CHANNEL_NAME: &str = "wechat";
pub const CHANNEL_VERSION: &str = "0.1.0";
pub const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
pub const DEFAULT_CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
pub const BOT_TYPE: &str = "3";
pub const LONG_POLL_TIMEOUT_MS: u64 = 35_000;
pub const MAX_CONSECUTIVE_FAILURES: usize = 3;
pub const BACKOFF_DELAY_MS: u64 = 30_000;
pub const RETRY_DELAY_MS: u64 = 2_000;
pub const MSG_TYPE_USER: i64 = 1;
pub const MSG_TYPE_BOT: i64 = 2;
pub const MSG_ITEM_TEXT: i64 = 1;
pub const MSG_ITEM_IMAGE: i64 = 2;
pub const MSG_ITEM_VOICE: i64 = 3;
pub const MSG_ITEM_FILE: i64 = 4;
pub const MSG_ITEM_VIDEO: i64 = 5;
pub const MSG_STATE_FINISH: i64 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountData {
    pub token: String,
    #[serde(rename = "baseUrl")]
    pub base_url: String,
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
    #[serde(rename = "savedAt")]
    pub saved_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QrCodeResponse {
    pub qrcode: String,
    pub qrcode_img_content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QrStatusResponse {
    pub status: String,
    pub bot_token: Option<String>,
    pub ilink_bot_id: Option<String>,
    pub baseurl: Option<String>,
    pub ilink_user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdnMedia {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypt_query_param: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aes_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypt_type: Option<i64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CdnMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CdnMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_media: Option<CdnMedia>,
    #[serde(rename = "aeskey")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aes_key_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mid_size: Option<i64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CdnMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CdnMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_media: Option<CdnMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_size: Option<i64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageItem {
    #[serde(rename = "type")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_type: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_item: Option<TextItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice_item: Option<VoiceItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_item: Option<ImageItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_item: Option<FileItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_item: Option<VideoItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_msg: Option<RefMessage>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeixinMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_list: Option<Vec<MessageItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time_ms: Option<i64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetUpdatesResp {
    pub ret: Option<i64>,
    pub errcode: Option<i64>,
    pub errmsg: Option<String>,
    pub msgs: Option<Vec<WeixinMessage>>,
    pub get_updates_buf: Option<String>,
}

pub fn log(message: &str) {
    let _ = writeln!(io::stderr(), "[wechat-channel] {message}");
}

pub fn log_error(message: &str) {
    let _ = writeln!(io::stderr(), "[wechat-channel] ERROR: {message}");
}

pub fn credentials_dir() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| "~".to_string());
    Path::new(&home)
        .join(".claude")
        .join("channels")
        .join("wechat")
}

pub fn credentials_file() -> PathBuf {
    credentials_dir().join("account.json")
}

pub fn sync_buf_file() -> PathBuf {
    credentials_dir().join("sync_buf.txt")
}

pub fn named_sync_buf_file(name: &str) -> PathBuf {
    credentials_dir().join(format!("sync_buf_{name}.txt"))
}

pub fn debug_dump_dir() -> PathBuf {
    credentials_dir().join("debug")
}

pub fn inbound_media_dir() -> PathBuf {
    credentials_dir().join("inbound_media")
}

fn sanitize_filename_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

pub fn write_debug_text(label: &str, sender_id: &str, content: &str) -> Result<PathBuf> {
    let dir = debug_dump_dir();
    fs::create_dir_all(&dir).context("failed to create debug dump dir")?;
    let filename = format!(
        "{}_{}_{}.txt",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
        sanitize_filename_component(label),
        sanitize_filename_component(sender_id)
    );
    let path = dir.join(filename);
    fs::write(&path, content).with_context(|| format!("failed to write debug dump {}", path.display()))?;
    Ok(path)
}

pub fn write_debug_json<T: Serialize>(label: &str, sender_id: &str, value: &T) -> Result<PathBuf> {
    let serialized = serde_json::to_string_pretty(value)?;
    write_debug_text(label, sender_id, &serialized)
}

fn message_storage_stem(msg: &WeixinMessage, sender_id: &str) -> String {
    let message_id = msg
        .message_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let create_time_ms = msg
        .create_time_ms
        .map(|ts| ts.to_string())
        .unwrap_or_else(|| Utc::now().timestamp_millis().to_string());
    format!(
        "{}_{}_{}",
        create_time_ms,
        sanitize_filename_component(sender_id),
        sanitize_filename_component(&message_id)
    )
}

fn normalize_extension(ext: &str) -> String {
    if ext.is_empty() {
        String::new()
    } else if ext.starts_with('.') {
        ext.to_ascii_lowercase()
    } else {
        format!(".{}", ext.to_ascii_lowercase())
    }
}

fn image_extension(bytes: &[u8]) -> &'static str {
    if bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff {
        ".jpg"
    } else if bytes.len() >= 8 && bytes[..8] == [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a] {
        ".png"
    } else if bytes.len() >= 6 && (&bytes[..6] == b"GIF87a" || &bytes[..6] == b"GIF89a") {
        ".gif"
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        ".webp"
    } else {
        ".jpg"
    }
}

fn normalize_voice_bytes(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() > 10 && bytes[0] == 0x02 && &bytes[1..10] == b"#!SILK_V3" {
        bytes[1..].to_vec()
    } else {
        bytes
    }
}

fn save_inbound_media_file(
    msg: &WeixinMessage,
    sender_id: &str,
    index: usize,
    stem_label: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<PathBuf> {
    let dir = inbound_media_dir();
    fs::create_dir_all(&dir).context("failed to create inbound media dir")?;
    let filename = format!(
        "{}_{}_{}{}",
        message_storage_stem(msg, sender_id),
        index,
        sanitize_filename_component(stem_label),
        normalize_extension(extension)
    );
    let path = dir.join(filename);
    fs::write(&path, bytes)
        .with_context(|| format!("failed to save inbound media {}", path.display()))?;
    Ok(path)
}

fn trim_opt(value: Option<&str>) -> Option<&str> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn parse_aes_key(aes_key_base64: &str, label: &str) -> Result<Vec<u8>> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(aes_key_base64.trim())
        .with_context(|| format!("{label}: aes_key base64 decode failed"))?;
    if decoded.len() == 16 {
        return Ok(decoded);
    }
    if decoded.len() == 32 {
        let as_text =
            std::str::from_utf8(&decoded).with_context(|| format!("{label}: aes_key is not utf8"))?;
        if as_text.chars().all(|ch| ch.is_ascii_hexdigit()) {
            let key = hex::decode(as_text)
                .with_context(|| format!("{label}: aes_key inner hex decode failed"))?;
            if key.len() == 16 {
                return Ok(key);
            }
        }
    }
    Err(anyhow!(
        "{label}: aes_key must decode to 16 bytes or 32-char hex, got {} bytes",
        decoded.len()
    ))
}

fn decrypt_aes_ecb_pkcs7(ciphertext: &[u8], key: &[u8], label: &str) -> Result<Vec<u8>> {
    if key.len() != 16 {
        return Err(anyhow!("{label}: aes key must be 16 bytes, got {}", key.len()));
    }
    if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
        return Err(anyhow!(
            "{label}: ciphertext length {} is not aligned to AES block size",
            ciphertext.len()
        ));
    }
    let cipher = Aes128::new_from_slice(key).map_err(|_| anyhow!("{label}: invalid aes key"))?;
    let mut out = ciphertext.to_vec();
    for chunk in out.chunks_exact_mut(16) {
        let block = GenericArray::from_mut_slice(chunk);
        cipher.decrypt_block(block);
    }
    let pad = *out
        .last()
        .ok_or_else(|| anyhow!("{label}: empty decrypted payload"))? as usize;
    if pad == 0 || pad > 16 || pad > out.len() {
        return Err(anyhow!("{label}: invalid pkcs7 padding"));
    }
    if !out[out.len() - pad..].iter().all(|&byte| byte as usize == pad) {
        return Err(anyhow!("{label}: invalid pkcs7 padding"));
    }
    out.truncate(out.len() - pad);
    Ok(out)
}

fn build_cdn_download_url(cdn_base: &str, encrypted_query_param: &str) -> String {
    format!(
        "{}/download?encrypted_query_param={}",
        cdn_base.trim_end_matches('/'),
        urlencoding::encode(encrypted_query_param)
    )
}

async fn fetch_cdn_bytes(
    client: &reqwest::Client,
    cdn_base: &str,
    encrypted_query_param: &str,
    label: &str,
) -> Result<Vec<u8>> {
    let url = build_cdn_download_url(cdn_base, encrypted_query_param);
    let response = timeout(Duration::from_secs(120), client.get(url).send())
        .await
        .with_context(|| format!("{label}: cdn request timed out"))??;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("{label}: read cdn response failed"))?;
    if !status.is_success() {
        let preview = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
        return Err(anyhow!("{label}: cdn http {status}: {preview}"));
    }
    if bytes.len() > (100 << 20) {
        return Err(anyhow!("{label}: cdn body exceeds 100 MiB"));
    }
    Ok(bytes.to_vec())
}

async fn download_and_decrypt_cdn(
    client: &reqwest::Client,
    cdn_base: &str,
    encrypted_query_param: &str,
    aes_key_base64: &str,
    label: &str,
) -> Result<Vec<u8>> {
    let key = parse_aes_key(aes_key_base64, label)?;
    let encrypted = fetch_cdn_bytes(client, cdn_base, encrypted_query_param, label).await?;
    decrypt_aes_ecb_pkcs7(&encrypted, &key, label)
}

fn image_decrypt_material(image: &ImageItem) -> (Option<&str>, Option<String>) {
    let enc = image
        .media
        .as_ref()
        .and_then(|media| trim_opt(media.encrypt_query_param.as_deref()));
    let key_from_hex = trim_opt(image.aes_key_hex.as_deref()).and_then(|hex_key| {
        let raw = hex::decode(hex_key).ok()?;
        if raw.len() == 16 {
            Some(base64::engine::general_purpose::STANDARD.encode(raw))
        } else {
            None
        }
    });
    let key = key_from_hex.or_else(|| {
        image
            .media
            .as_ref()
            .and_then(|media| trim_opt(media.aes_key.as_deref()))
            .map(ToOwned::to_owned)
    });
    (enc, key)
}

pub async fn extract_and_save_inbound_media(
    client: &reqwest::Client,
    cdn_base: &str,
    msg: &WeixinMessage,
    sender_id: &str,
) -> Result<Vec<PathBuf>> {
    let Some(items) = msg.item_list.as_ref() else {
        return Ok(Vec::new());
    };
    let mut saved = Vec::new();
    let mut seen_enc = HashSet::new();

    for (index, item) in items.iter().enumerate() {
        match item.item_type {
            Some(MSG_ITEM_IMAGE) => {
                let Some(image) = item.image_item.as_ref() else {
                    continue;
                };
                let (Some(enc), key_b64) = image_decrypt_material(image) else {
                    continue;
                };
                if !seen_enc.insert(enc.to_string()) {
                    continue;
                }
                let bytes = if let Some(key_b64) = key_b64 {
                    download_and_decrypt_cdn(client, cdn_base, enc, &key_b64, "inbound image").await?
                } else {
                    fetch_cdn_bytes(client, cdn_base, enc, "inbound image plain").await?
                };
                saved.push(save_inbound_media_file(
                    msg,
                    sender_id,
                    index,
                    "image",
                    image_extension(&bytes),
                    &bytes,
                )?);
            }
            Some(MSG_ITEM_FILE) => {
                let Some(file) = item.file_item.as_ref() else {
                    continue;
                };
                let Some(media) = file.media.as_ref() else {
                    continue;
                };
                let (Some(enc), Some(aes_key)) = (
                    trim_opt(media.encrypt_query_param.as_deref()),
                    trim_opt(media.aes_key.as_deref()),
                ) else {
                    continue;
                };
                if !seen_enc.insert(enc.to_string()) {
                    continue;
                }
                let bytes =
                    download_and_decrypt_cdn(client, cdn_base, enc, aes_key, "inbound file").await?;
                let file_name = file.file_name.as_deref().unwrap_or("attachment.bin");
                let path = save_inbound_media_file(
                    msg,
                    sender_id,
                    index,
                    Path::new(file_name)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("file"),
                    Path::new(file_name)
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or("bin"),
                    &bytes,
                )?;
                saved.push(path);
            }
            Some(MSG_ITEM_VIDEO) => {
                let Some(video) = item.video_item.as_ref() else {
                    continue;
                };
                let Some(media) = video.media.as_ref() else {
                    continue;
                };
                let (Some(enc), Some(aes_key)) = (
                    trim_opt(media.encrypt_query_param.as_deref()),
                    trim_opt(media.aes_key.as_deref()),
                ) else {
                    continue;
                };
                if !seen_enc.insert(enc.to_string()) {
                    continue;
                }
                let bytes =
                    download_and_decrypt_cdn(client, cdn_base, enc, aes_key, "inbound video").await?;
                saved.push(save_inbound_media_file(
                    msg,
                    sender_id,
                    index,
                    "video",
                    "mp4",
                    &bytes,
                )?);
            }
            Some(MSG_ITEM_VOICE) => {
                let Some(voice) = item.voice_item.as_ref() else {
                    continue;
                };
                let Some(media) = voice.media.as_ref() else {
                    continue;
                };
                let (Some(enc), Some(aes_key)) = (
                    trim_opt(media.encrypt_query_param.as_deref()),
                    trim_opt(media.aes_key.as_deref()),
                ) else {
                    continue;
                };
                if !seen_enc.insert(enc.to_string()) {
                    continue;
                }
                let bytes =
                    download_and_decrypt_cdn(client, cdn_base, enc, aes_key, "inbound voice").await?;
                let bytes = normalize_voice_bytes(bytes);
                saved.push(save_inbound_media_file(
                    msg,
                    sender_id,
                    index,
                    "voice",
                    "silk",
                    &bytes,
                )?);
            }
            _ => {}
        }
    }

    Ok(saved)
}

pub fn load_credentials() -> Option<AccountData> {
    let path = credentials_file();
    let contents = fs::read_to_string(path).ok()?;
    serde_json::from_str(&contents).ok()
}

pub fn save_credentials(data: &AccountData) -> Result<()> {
    let dir = credentials_dir();
    fs::create_dir_all(&dir).context("failed to create credentials dir")?;
    let path = credentials_file();
    fs::write(&path, serde_json::to_vec_pretty(data)?).context("failed to write credentials")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&path, perms)?;
    }
    Ok(())
}

pub fn random_wechat_uin() -> String {
    let mut bytes = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut bytes);
    let value = u32::from_be_bytes(bytes);
    base64::engine::general_purpose::STANDARD.encode(value.to_string())
}

pub fn generate_client_id() -> String {
    let mut bytes = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!(
        "claude-code-wechat:{}-{}",
        Utc::now().timestamp_millis(),
        hex::encode(bytes)
    )
}

pub fn build_headers(token: Option<&str>, body: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        "AuthorizationType",
        HeaderValue::from_static("ilink_bot_token"),
    );
    headers.insert("X-WECHAT-UIN", HeaderValue::from_str(&random_wechat_uin())?);
    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&body.len().to_string())?,
    );
    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token.trim()))?,
        );
    }
    Ok(headers)
}

pub async fn api_fetch(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &str,
    body: String,
    token: Option<&str>,
    timeout_ms: u64,
) -> Result<String> {
    let base = if base_url.ends_with('/') {
        base_url.to_string()
    } else {
        format!("{base_url}/")
    };
    let url = format!("{base}{endpoint}");
    let headers = build_headers(token, &body)?;
    let request = client.post(url).headers(headers).body(body);
    let response = timeout(Duration::from_millis(timeout_ms), request.send())
        .await
        .context("request timed out")??;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(anyhow!("HTTP {}: {}", status, text));
    }
    Ok(text)
}

pub async fn fetch_qrcode(client: &reqwest::Client, base_url: &str) -> Result<QrCodeResponse> {
    let base = if base_url.ends_with('/') {
        base_url.to_string()
    } else {
        format!("{base_url}/")
    };
    let url = format!("{base}ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}");
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("QR fetch failed: {}", status));
    }
    Ok(response.json::<QrCodeResponse>().await?)
}

pub async fn poll_qr_status(
    client: &reqwest::Client,
    base_url: &str,
    qrcode: &str,
) -> Result<QrStatusResponse> {
    let base = if base_url.ends_with('/') {
        base_url.to_string()
    } else {
        format!("{base_url}/")
    };
    let url = format!(
        "{base}ilink/bot/get_qrcode_status?qrcode={}",
        urlencoding::encode(qrcode)
    );
    let request = client.get(url).header("iLink-App-ClientVersion", "1");
    match timeout(Duration::from_millis(35_000), request.send()).await {
        Ok(Ok(response)) => {
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("QR status failed: {}", status));
            }
            Ok(response.json::<QrStatusResponse>().await?)
        }
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Ok(QrStatusResponse {
            status: "wait".to_string(),
            bot_token: None,
            ilink_bot_id: None,
            baseurl: None,
            ilink_user_id: None,
        }),
    }
}

pub async fn do_qr_login(client: &reqwest::Client, base_url: &str) -> Result<Option<AccountData>> {
    log("正在获取微信登录二维码...");
    let qr_resp = fetch_qrcode(client, base_url).await?;
    log("\n请使用微信扫描以下二维码：\n");
    if qr2term::print_qr(qr_resp.qrcode_img_content.as_bytes()).is_err() {
        log(&format!("二维码链接: {}", qr_resp.qrcode_img_content));
    }
    log("等待扫码...");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(480);
    let mut scanned_printed = false;

    while tokio::time::Instant::now() < deadline {
        let status = poll_qr_status(client, base_url, &qr_resp.qrcode).await?;
        match status.status.as_str() {
            "wait" => {}
            "scaned" => {
                if !scanned_printed {
                    log("👀 已扫码，请在微信中确认...");
                    scanned_printed = true;
                }
            }
            "expired" => {
                log("二维码已过期，请重新启动。");
                return Ok(None);
            }
            "confirmed" => {
                let account_id = status
                    .ilink_bot_id
                    .ok_or_else(|| anyhow!("登录确认但未返回 bot id"))?;
                let token = status
                    .bot_token
                    .ok_or_else(|| anyhow!("登录确认但未返回 token"))?;
                let account = AccountData {
                    token,
                    base_url: status.baseurl.unwrap_or_else(|| base_url.to_string()),
                    account_id,
                    user_id: status.ilink_user_id,
                    saved_at: Utc::now().to_rfc3339(),
                };
                save_credentials(&account)?;
                log("✅ 微信连接成功！");
                return Ok(Some(account));
            }
            _ => {}
        }
        sleep(Duration::from_secs(1)).await;
    }

    log("登录超时");
    Ok(None)
}

pub fn extract_text_from_message(msg: &WeixinMessage) -> String {
    let Some(items) = &msg.item_list else {
        return String::new();
    };

    for item in items {
        match item.item_type {
            Some(MSG_ITEM_TEXT) => {
                if let Some(text) = item.text_item.as_ref().and_then(|item| item.text.clone()) {
                    if let Some(reference) = &item.ref_msg {
                        if let Some(title) = &reference.title {
                            return format!("[引用: {title}]\n{text}");
                        }
                    }
                    return text;
                }
            }
            Some(MSG_ITEM_VOICE) => {
                if let Some(text) = item.voice_item.as_ref().and_then(|item| item.text.clone()) {
                    return text;
                }
            }
            _ => {}
        }
    }

    String::new()
}

pub async fn get_updates(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    get_updates_buf: &str,
) -> Result<GetUpdatesResp> {
    let raw = api_fetch(
        client,
        base_url,
        "ilink/bot/getupdates",
        json!({
            "get_updates_buf": get_updates_buf,
            "base_info": { "channel_version": CHANNEL_VERSION }
        })
        .to_string(),
        Some(token),
        LONG_POLL_TIMEOUT_MS,
    )
    .await?;
    Ok(serde_json::from_str(&raw)?)
}

pub async fn send_text_message(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    to: &str,
    text: &str,
    context_token: &str,
) -> Result<String> {
    let client_id = generate_client_id();
    let payload = build_outbound_payload(
        &client_id,
        to,
        &[MessageItem {
            item_type: Some(MSG_ITEM_TEXT),
            text_item: Some(TextItem {
                text: Some(text.to_string()),
                extra: Map::new(),
            }),
            voice_item: None,
            image_item: None,
            file_item: None,
            video_item: None,
            ref_msg: None,
            extra: Map::new(),
        }],
        context_token,
    );
    post_send_message(client, base_url, token, &payload).await?;
    Ok(client_id)
}

pub async fn send_message_items(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    to: &str,
    items: &[MessageItem],
    context_token: &str,
) -> Result<String> {
    let client_id = generate_client_id();
    let payload = build_outbound_payload(&client_id, to, items, context_token);
    post_send_message(client, base_url, token, &payload).await?;
    Ok(client_id)
}

pub fn build_outbound_payload(
    client_id: &str,
    to: &str,
    items: &[MessageItem],
    context_token: &str,
) -> Value {
    json!({
        "msg": {
            "from_user_id": "",
            "to_user_id": to,
            "client_id": client_id,
            "message_type": MSG_TYPE_BOT,
            "message_state": MSG_STATE_FINISH,
            "item_list": items,
            "context_token": context_token,
        },
        "base_info": { "channel_version": CHANNEL_VERSION }
    })
}

pub async fn post_send_message(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    payload: &Value,
) -> Result<String> {
    api_fetch(
        client,
        base_url,
        "ilink/bot/sendmessage",
        payload.to_string(),
        Some(token),
        15_000,
    )
    .await
}

#[derive(Clone)]
pub struct SharedState {
    pub account: Arc<Mutex<Option<AccountData>>>,
    pub context_tokens: Arc<Mutex<HashMap<String, String>>>,
    pub stdout: Arc<Mutex<io::Stdout>>,
    pub client: reqwest::Client,
}

impl SharedState {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            account: Arc::new(Mutex::new(None)),
            context_tokens: Arc::new(Mutex::new(HashMap::new())),
            stdout: Arc::new(Mutex::new(io::stdout())),
            client,
        }
    }

    pub async fn send_json(&self, payload: &Value) -> Result<()> {
        let serialized = serde_json::to_vec(payload)?;
        let mut stdout = self.stdout.lock().await;
        write!(stdout, "Content-Length: {}\r\n\r\n", serialized.len())?;
        stdout.write_all(&serialized)?;
        stdout.flush()?;
        Ok(())
    }

    pub async fn notify_channel(&self, content: &str, sender_id: &str) -> Result<()> {
        let sender = sender_id.split('@').next().unwrap_or(sender_id);
        self.send_json(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/claude/channel",
            "params": {
                "content": content,
                "meta": {
                    "sender": sender,
                    "sender_id": sender_id,
                }
            }
        }))
        .await
    }
}

pub async fn handle_mcp_messages(state: SharedState) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut reader = AsyncBufReader::new(stdin);

    loop {
        let mut content_length: Option<usize> = None;
        loop {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                return Ok(());
            }
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("Content-Length") {
                    content_length = Some(value.trim().parse()?);
                }
            }
        }

        let length = content_length.ok_or_else(|| anyhow!("missing Content-Length header"))?;
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).await?;
        let message: Value = serde_json::from_slice(&body)?;
        if let Some(method) = message.get("method").and_then(Value::as_str) {
            match method {
                "initialize" => {
                    let id = message.get("id").cloned().unwrap_or(Value::Null);
                    let instructions = [
                        "Messages from WeChat users arrive as <channel source=\"wechat\" sender=\"...\" sender_id=\"...\">",
                        "Reply using the wechat_reply tool. You MUST pass the sender_id from the inbound tag.",
                        "Messages are from real WeChat users via the WeChat ClawBot interface.",
                        "Respond naturally in Chinese unless the user writes in another language.",
                        "Keep replies concise — WeChat is a chat app, not an essay platform.",
                        "Strip markdown formatting (WeChat doesn't render it). Use plain text.",
                    ]
                    .join("\n");
                    state.send_json(&json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {
                                "experimental": { "claude/channel": {} },
                                "tools": {}
                            },
                            "serverInfo": {
                                "name": CHANNEL_NAME,
                                "version": CHANNEL_VERSION
                            },
                            "instructions": instructions
                        }
                    })).await?;
                }
                "notifications/initialized" => {}
                "tools/list" => {
                    let id = message.get("id").cloned().unwrap_or(Value::Null);
                    state.send_json(&json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "tools": [{
                                "name": "wechat_reply",
                                "description": "Send a text reply back to the WeChat user",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "sender_id": {
                                            "type": "string",
                                            "description": "The sender_id from the inbound <channel> tag (xxx@im.wechat format)"
                                        },
                                        "text": {
                                            "type": "string",
                                            "description": "The plain-text message to send (no markdown)"
                                        }
                                    },
                                    "required": ["sender_id", "text"]
                                }
                            }]
                        }
                    })).await?;
                }
                "tools/call" => {
                    let id = message.get("id").cloned().unwrap_or(Value::Null);
                    let params = message.get("params").cloned().unwrap_or(Value::Null);
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if name != "wechat_reply" {
                        state.send_json(&json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": { "code": -32601, "message": format!("unknown tool: {name}") }
                        })).await?;
                        continue;
                    }

                    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
                    let sender_id = arguments
                        .get("sender_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let text = arguments
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let account = state.account.lock().await.clone();
                    let result_text = if let Some(account) = account {
                        let context_token =
                            state.context_tokens.lock().await.get(sender_id).cloned();
                        if let Some(context_token) = context_token {
                            match send_text_message(
                                &state.client,
                                &account.base_url,
                                &account.token,
                                sender_id,
                                text,
                                &context_token,
                            )
                            .await
                            {
                                Ok(_) => "sent".to_string(),
                                Err(err) => format!("send failed: {err}"),
                            }
                        } else {
                            format!("error: no context_token for {sender_id}. The user may need to send a message first.")
                        }
                    } else {
                        "error: not logged in".to_string()
                    };

                    state
                        .send_json(&json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "content": [{ "type": "text", "text": result_text }]
                            }
                        }))
                        .await?;
                }
                _ => {
                    if message.get("id").is_some() {
                        state.send_json(&json!({
                            "jsonrpc": "2.0",
                            "id": message.get("id").cloned().unwrap_or(Value::Null),
                            "error": { "code": -32601, "message": format!("unsupported method: {method}") }
                        })).await?;
                    }
                }
            }
        }
    }
}

pub async fn start_polling(state: SharedState, account: AccountData) -> Result<()> {
    let mut get_updates_buf = fs::read_to_string(sync_buf_file()).unwrap_or_default();
    if !get_updates_buf.is_empty() {
        log(&format!(
            "恢复上次同步状态 ({} bytes)",
            get_updates_buf.len()
        ));
    }
    log("开始监听微信消息...");

    let mut consecutive_failures = 0usize;
    loop {
        match get_updates(
            &state.client,
            &account.base_url,
            &account.token,
            &get_updates_buf,
        )
        .await
        {
            Ok(response) => {
                let is_error = response.ret.unwrap_or(0) != 0 || response.errcode.unwrap_or(0) != 0;
                if is_error {
                    consecutive_failures += 1;
                    log_error(&format!(
                        "getUpdates 失败: ret={} errcode={} errmsg={}",
                        response.ret.unwrap_or_default(),
                        response.errcode.unwrap_or_default(),
                        response.errmsg.unwrap_or_default()
                    ));
                    let delay = if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        consecutive_failures = 0;
                        BACKOFF_DELAY_MS
                    } else {
                        RETRY_DELAY_MS
                    };
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }

                consecutive_failures = 0;
                if let Some(buf) = response.get_updates_buf {
                    get_updates_buf = buf;
                    let _ = fs::write(sync_buf_file(), &get_updates_buf);
                }

                for msg in response.msgs.unwrap_or_default() {
                    if msg.message_type != Some(MSG_TYPE_USER) {
                        continue;
                    }
                    let sender_id = msg
                        .from_user_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    match extract_and_save_inbound_media(
                        &state.client,
                        DEFAULT_CDN_BASE_URL,
                        &msg,
                        &sender_id,
                    )
                    .await
                    {
                        Ok(paths) if !paths.is_empty() => {
                            for path in paths {
                                log(&format!("已保存入站媒体: {}", path.display()));
                            }
                        }
                        Ok(_) => {}
                        Err(err) => log_error(&format!(
                            "保存入站媒体失败: from={} error={err}",
                            sender_id
                        )),
                    }
                    let text = extract_text_from_message(&msg);
                    if text.is_empty() {
                        continue;
                    }
                    if let Some(token) = msg.context_token {
                        state
                            .context_tokens
                            .lock()
                            .await
                            .insert(sender_id.clone(), token);
                    }
                    log(&format!(
                        "收到消息: from={} text={}...",
                        sender_id,
                        text.chars().take(50).collect::<String>()
                    ));
                    state.notify_channel(&text, &sender_id).await?;
                }
            }
            Err(err) => {
                consecutive_failures += 1;
                log_error(&format!("轮询异常: {err}"));
                let delay = if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    consecutive_failures = 0;
                    BACKOFF_DELAY_MS
                } else {
                    RETRY_DELAY_MS
                };
                sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

pub fn describe_message_items(items: &[MessageItem]) -> String {
    let mut kinds = Vec::new();
    for item in items {
        let label = match item.item_type {
            Some(MSG_ITEM_TEXT) => "text",
            Some(MSG_ITEM_IMAGE) => "image",
            Some(MSG_ITEM_VOICE) => "voice",
            Some(MSG_ITEM_FILE) => "file",
            Some(MSG_ITEM_VIDEO) => "video",
            Some(other) => {
                if item.file_item.is_some() || item.extra.keys().any(|key| key.contains("file")) {
                    "file"
                } else if other == 6 {
                    "file"
                } else {
                    "unknown"
                }
            }
            None => {
                if item.file_item.is_some() || item.extra.keys().any(|key| key.contains("file")) {
                    "file"
                } else if item.image_item.is_some() {
                    "image"
                } else if item.video_item.is_some() {
                    "video"
                } else if item.voice_item.is_some() {
                    "voice"
                } else {
                    "unknown"
                }
            }
        };
        kinds.push(label);
    }
    kinds.join(",")
}

pub async fn start_echo_polling(
    client: &reqwest::Client,
    account: &AccountData,
    sync_file: &Path,
) -> Result<()> {
    let mut get_updates_buf = fs::read_to_string(sync_file).unwrap_or_default();
    if !get_updates_buf.is_empty() {
        log(&format!(
            "恢复 echo 同步状态 ({} bytes)",
            get_updates_buf.len()
        ));
    }
    log("开始监听微信消息并原样回声...");

    let mut consecutive_failures = 0usize;
    loop {
        match get_updates(client, &account.base_url, &account.token, &get_updates_buf).await {
            Ok(response) => {
                let is_error = response.ret.unwrap_or(0) != 0 || response.errcode.unwrap_or(0) != 0;
                if is_error {
                    consecutive_failures += 1;
                    log_error(&format!(
                        "getUpdates 失败: ret={} errcode={} errmsg={}",
                        response.ret.unwrap_or_default(),
                        response.errcode.unwrap_or_default(),
                        response.errmsg.unwrap_or_default()
                    ));
                    let delay = if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        consecutive_failures = 0;
                        BACKOFF_DELAY_MS
                    } else {
                        RETRY_DELAY_MS
                    };
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }

                consecutive_failures = 0;
                if let Some(buf) = response.get_updates_buf {
                    get_updates_buf = buf;
                    if let Some(parent) = sync_file.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let _ = fs::write(sync_file, &get_updates_buf);
                }

                for msg in response.msgs.unwrap_or_default() {
                    if msg.message_type != Some(MSG_TYPE_USER) {
                        continue;
                    }

                    let sender_id = msg
                        .from_user_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    match extract_and_save_inbound_media(client, DEFAULT_CDN_BASE_URL, &msg, &sender_id)
                        .await
                    {
                        Ok(paths) if !paths.is_empty() => {
                            for path in paths {
                                log(&format!("已保存入站媒体: {}", path.display()));
                            }
                        }
                        Ok(_) => {}
                        Err(err) => log_error(&format!(
                            "保存入站媒体失败: from={} error={err}",
                            sender_id
                        )),
                    }
                    let Some(context_token) = msg.context_token.as_deref() else {
                        log_error(&format!("跳过无 context_token 的消息: from={sender_id}"));
                        continue;
                    };
                    let Some(items) = msg.item_list.as_ref() else {
                        continue;
                    };
                    if items.is_empty() {
                        continue;
                    }

                    let summary = describe_message_items(items);
                    log(&format!("收到消息: from={sender_id} kinds={summary}"));
                    match write_debug_json("incoming", &sender_id, &msg) {
                        Ok(path) => log(&format!("已写入入站消息调试文件: {}", path.display())),
                        Err(err) => log_error(&format!("写入入站消息调试文件失败: {err}")),
                    }

                    let client_id = generate_client_id();
                    let payload = build_outbound_payload(&client_id, &sender_id, items, context_token);
                    match write_debug_json("outgoing", &sender_id, &payload) {
                        Ok(path) => log(&format!("已写入出站 payload 调试文件: {}", path.display())),
                        Err(err) => log_error(&format!("写入出站 payload 调试文件失败: {err}")),
                    }

                    match post_send_message(client, &account.base_url, &account.token, &payload).await {
                        Ok(response) => {
                            match write_debug_text("send_response", &sender_id, &response) {
                                Ok(path) => log(&format!("已写入发送响应调试文件: {}", path.display())),
                                Err(err) => log_error(&format!("写入发送响应调试文件失败: {err}")),
                            }
                            log(&format!("已回声消息给 {sender_id}"));
                        }
                        Err(err) => {
                            let _ = write_debug_text("send_error", &sender_id, &format!("{err:#}"));
                            let text = extract_text_from_message(&msg);
                            if text.is_empty() {
                                log_error(&format!(
                                    "回声失败且无法降级为文本: from={sender_id} error={err}"
                                ));
                                continue;
                            }

                            log_error(&format!(
                                "原样回声失败，尝试文本降级: from={sender_id} error={err}"
                            ));
                            match send_text_message(
                                client,
                                &account.base_url,
                                &account.token,
                                &sender_id,
                                &text,
                                context_token,
                            )
                            .await
                            {
                                Ok(_) => log(&format!("已使用文本降级回声给 {sender_id}")),
                                Err(fallback_err) => log_error(&format!(
                                    "文本降级回声失败: from={sender_id} error={fallback_err}"
                                )),
                            }
                        }
                    }
                }
            }
            Err(err) => {
                consecutive_failures += 1;
                log_error(&format!("轮询异常: {err}"));
                let delay = if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    consecutive_failures = 0;
                    BACKOFF_DELAY_MS
                } else {
                    RETRY_DELAY_MS
                };
                sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

pub fn prompt_yes_no(prompt: &str) -> Result<bool> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut input = String::new();
    BufReader::new(io::stdin()).read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}
