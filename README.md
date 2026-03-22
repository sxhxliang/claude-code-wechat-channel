# Claude Code WeChat Channel

将微信消息桥接到 Claude Code 会话的 Rust 版 Channel 插件。

基于微信官方 ClawBot ilink API（与 `@tencent-weixin/openclaw-weixin` 使用相同协议），让你在微信中直接与 Claude Code 对话。

## 工作原理

```text
微信 (iOS) → WeChat ClawBot → ilink API → [本插件] → Claude Code Session
                                                  ↕
Claude Code ← MCP Channel Protocol ← wechat_reply tool
```

## 前置要求

- [Rust](https://www.rust-lang.org/tools/install) >= 1.77
- [Cargo](https://doc.rust-lang.org/cargo/)（随 Rust 一起安装）
- [Claude Code](https://claude.com/claude-code) >= 2.1.80
- claude.ai 账号登录（不支持 API key）
- 微信 iOS 最新版（需支持 ClawBot 插件）

## 快速开始

### 1. 构建项目

```bash
git clone https://github.com/anthropics/claude-code-wechat-channel.git
cd claude-code-wechat-channel
cargo build --release
```

### 2. 微信扫码登录

```bash
cargo run --bin setup
```

终端会显示二维码，用微信扫描并确认。凭据保存到 `~/.claude/channels/wechat/account.json`。

### 3. 启动 Claude Code + WeChat 通道

```bash
claude --dangerously-load-development-channels server:wechat
```

如果你在本地开发调试，也可以直接运行：

```bash
cargo run --bin wechat-channel
```

### 4. 在微信中发消息

打开微信，找到 ClawBot 对话，发送消息。消息会出现在 Claude Code 终端中，Claude 的回复会自动发回微信。

## 文件说明

| 文件 | 说明 |
|------|------|
| `src/bin/wechat-channel.rs` | MCP Channel 服务器主程序 |
| `src/bin/setup.rs` | 独立的微信扫码登录工具 |
| `src/lib.rs` | WeChat API、凭据、轮询与 MCP 公共逻辑 |
| `Cargo.toml` | Rust 项目配置 |

## 技术细节

- **消息接收**: 通过 `ilink/bot/getupdates` 长轮询获取微信消息
- **消息发送**: 通过 `ilink/bot/sendmessage` 发送回复
- **认证**: 使用 `ilink/bot/get_bot_qrcode` QR 码登录获取 Bearer Token
- **协议**: 通过 stdio 实现 MCP (Model Context Protocol) Channel 扩展

## 注意事项

- 当前为 research preview 阶段，需要使用 `--dangerously-load-development-channels` 标志
- Claude Code 会话关闭后通道也会断开
- 微信 ClawBot 目前仅支持 iOS 最新版
- 每个 ClawBot 只能连接一个 agent 实例

## License

MIT
