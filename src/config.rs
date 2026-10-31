//! 从 BOT 的 config.yaml 读取端口与 token。
//!
//! 用户要求：读 BOT 的配置，端口为 BOT 端口 +1，token 拒绝默认值。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// BOT 的默认 token —— 等于这个值说明用户从未改过，等于没有鉴权。
pub const DEFAULT_TOKEN: &str = "miliastra-default-access-token";

#[derive(Debug, Deserialize)]
struct BotConfig {
    http: HttpSection,
}

#[derive(Debug, Deserialize)]
struct HttpSection {
    port: u16,
    access_token: String,
}

/// 本服务的运行参数。
#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub token: String,
    /// BOT 自己的游戏 UID。用于识别"自己发的消息"。
    pub self_uid: u32,
}

/// 默认的 BOT 配置路径（相对于本程序所在目录的上一级）。
pub fn default_config_path() -> PathBuf {
    PathBuf::from("../Miliastra-Wonderland-Music/config.yaml")
}

/// 命令行直接给出的覆盖值。
///
/// BOT 的配置存在数据库里，不一定有 yaml 文件可读，所以允许直接传。
#[derive(Debug, Default)]
pub struct Overrides {
    pub port: Option<u16>,
    pub token: Option<String>,
}

/// 从 BOT 的 config.yaml 加载，并允许命令行覆盖。
pub fn load(path: &Path, self_uid: u32, overrides: &Overrides) -> Result<Config> {
    // 端口与令牌都给全了就不碰配置文件：BOT 的配置存在数据库里，
    // 没有 yaml 可以读。
    if let (Some(port), Some(token)) = (overrides.port, overrides.token.as_deref()) {
        validate_token(Path::new("--token"), token)?;
        return Ok(Config {
            port,
            token: token.to_owned(),
            self_uid,
        });
    }
    let (port, token) = load_raw(path)?;
    let port = match overrides.port {
        Some(port) => port,
        None => port.checked_add(1).context("BOT 端口 +1 溢出")?,
    };
    let token = overrides.token.clone().unwrap_or(token);
    validate_token(path, &token)?;
    Ok(Config {
        port,
        token,
        self_uid,
    })
}

/// 只读配置里的端口与 token。
fn load_raw(path: &Path) -> Result<(u16, String)> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
    let bot: BotConfig = serde_yaml::from_str(&text)
        .with_context(|| format!("解析配置文件失败: {}", path.display()))?;

    // 只读取，不校验：调用方可能用 --token 覆盖，这时 yaml 里的占位值无害。
    Ok((bot.http.port, bot.http.access_token.trim().to_owned()))
}

/// token 合法性检查。
fn validate_token(path: &Path, token: &str) -> Result<()> {
    if token.trim().is_empty() {
        bail!(
            "{} 里的 http.access_token 为空：请先启动一次 BOT 让它自动生成，或手动填入随机值",
            path.display()
        );
    }
    if token == DEFAULT_TOKEN {
        bail!(
            "{} 里的 http.access_token 仍是默认值：这等于没有鉴权，请改成随机值后重试",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("config.yaml");
        std::fs::write(&path, body).expect("写测试配置");
        path
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "Miliastra-Wonderland-ChatBridge-cfg-{tag}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("建临时目录");
        dir
    }

    #[test]
    fn port_is_bot_port_plus_one() {
        let dir = tmpdir("port");
        let path = write(&dir, "http:\n  port: 18888\n  access_token: abc123\n");
        let cfg = load(&path, 42, &Overrides::default()).expect("加载");
        assert_eq!(cfg.port, 18889);
        assert_eq!(cfg.token, "abc123");
        assert_eq!(cfg.self_uid, 42);
    }

    #[test]
    fn rejects_empty_token() {
        let dir = tmpdir("empty");
        let path = write(&dir, "http:\n  port: 18888\n  access_token: \"\"\n");
        let err = load(&path, 1, &Overrides::default()).expect_err("空 token 必须拒绝");
        assert!(err.to_string().contains("为空"), "{err}");
    }

    #[test]
    fn rejects_default_token() {
        let dir = tmpdir("default");
        let path = write(
            &dir,
            "http:\n  port: 18888\n  access_token: miliastra-default-access-token\n",
        );
        let err = load(&path, 1, &Overrides::default()).expect_err("默认 token 必须拒绝");
        assert!(err.to_string().contains("默认值"), "{err}");
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = load(
            Path::new("/definitely/not/here.yaml"),
            1,
            &Overrides::default(),
        )
        .expect_err("文件不存在");
        assert!(err.to_string().contains("读取配置文件失败"), "{err}");
    }
}
