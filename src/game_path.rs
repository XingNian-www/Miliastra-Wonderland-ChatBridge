//! 游戏可执行文件定位（跨平台纯逻辑 + 平台注册表查询分离）。
//!
//! 与 Miliastra-Wonderland-Music 的 `adapters/game_path.rs` 同构：
//! 路径校验与候选生成是纯字符串/路径逻辑，放在跨平台模块里可测；
//! 只有"从启动器注册表读安装目录"这一步是 Windows 专属（见 `inject`）。
//!
//! 之前 `locate_game` 里写死三处路径，换个盘或非默认安装就找不到游戏。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// 游戏可执行文件名：国服 YuanShen.exe，国际服 GenshinImpact.exe。
pub const GAME_EXE_CANDIDATES: &[&str] = &["YuanShen.exe", "GenshinImpact.exe"];

/// 常见默认安装位置，作为注册表缺失时的兜底。
pub const DEFAULT_INSTALL_DIRS: &[&str] = &[
    r"D:\Genshin Impact Game",
    r"C:\Program Files\Genshin Impact\Genshin Impact Game",
    r"C:\Program Files\YuanShen Game",
];

/// 在目录里按候选名找游戏可执行文件。
pub fn executable_in_dir(dir: &Path) -> Option<PathBuf> {
    for name in GAME_EXE_CANDIDATES {
        let path = dir.join(name);
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

/// 显式路径：允许给可执行文件本身或其所在目录。
///
/// 只接受上面的候选文件名，避免这个入口变成"启动任意程序"的通道。
pub fn explicit_game_path(path: &Path) -> Result<PathBuf> {
    if path.is_dir() {
        return executable_in_dir(path).ok_or_else(|| {
            anyhow::anyhow!(
                "--game-path 目录里没有 {}（{}）",
                GAME_EXE_CANDIDATES.join(" / "),
                path.display()
            )
        });
    }
    anyhow::ensure!(
        path.is_file(),
        "--game-path 指向的路径不存在: {}",
        path.display()
    );
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    let allowed = GAME_EXE_CANDIDATES
        .iter()
        .any(|candidate| candidate.to_ascii_lowercase() == name);
    anyhow::ensure!(
        allowed,
        "--game-path 必须指向游戏可执行文件（允许: {}）",
        GAME_EXE_CANDIDATES.join(", ")
    );
    Ok(path.to_path_buf())
}

/// 按优先级解析游戏可执行文件。
///
/// 1. 显式指定的 `--game-path`（文件或目录）
/// 2. 启动器注册表里的安装目录（由调用方传入，国服 / 国际服）
/// 3. 常见默认安装位置
///
/// `registry_dirs` 由平台层提供，非 Windows 传空即可。
pub fn resolve(explicit: Option<&Path>, registry_dirs: Vec<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        let found = explicit_game_path(path).context("校验 --game-path 失败")?;
        tracing::info!("游戏路径来自 --game-path: {}", found.display());
        return Ok(found);
    }
    for dir in registry_dirs {
        if let Some(found) = executable_in_dir(&dir) {
            tracing::info!(
                "游戏路径来自启动器注册表: {} -> {}",
                dir.display(),
                found.display()
            );
            return Ok(found);
        }
        tracing::debug!("注册表安装目录里没有游戏可执行文件: {}", dir.display());
    }
    for dir in DEFAULT_INSTALL_DIRS {
        let dir = Path::new(dir);
        if let Some(found) = executable_in_dir(dir) {
            tracing::info!("游戏路径来自默认安装位置: {}", found.display());
            return Ok(found);
        }
    }
    anyhow::bail!(
        "未找到游戏可执行文件（{}）。请确认已安装国服/国际服客户端，或用 --game-path 指定游戏 exe 或其所在目录",
        GAME_EXE_CANDIDATES.join(" / ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mwc-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn executable_in_dir_matches_candidate_names() {
        let dir = scratch("game-scan");
        assert!(executable_in_dir(&dir).is_none(), "空目录里不应找到游戏");

        let intl = dir.join(GAME_EXE_CANDIDATES[1]);
        std::fs::write(&intl, b"stub").unwrap();
        assert_eq!(executable_in_dir(&dir).as_deref(), Some(intl.as_path()));

        // 国服名优先于国际服名。
        let cn = dir.join(GAME_EXE_CANDIDATES[0]);
        std::fs::write(&cn, b"stub").unwrap();
        assert_eq!(executable_in_dir(&dir).as_deref(), Some(cn.as_path()));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn explicit_game_path_accepts_dir_and_allowlisted_file() {
        let dir = scratch("game-explicit");
        let exe = dir.join("YuanShen.exe");
        std::fs::write(&exe, b"stub").unwrap();

        assert_eq!(explicit_game_path(&dir).unwrap(), exe);
        assert_eq!(explicit_game_path(&exe).unwrap(), exe);

        // 非游戏可执行文件必须被拒绝，避免变成任意程序启动通道。
        let other = dir.join("notepad.exe");
        std::fs::write(&other, b"stub").unwrap();
        let error = explicit_game_path(&other).expect_err("白名单外的 exe 必须被拒绝");
        assert!(error.to_string().contains("必须指向游戏可执行文件"));

        let error = explicit_game_path(&dir.join("missing")).expect_err("不存在的路径必须报错");
        assert!(error.to_string().contains("不存在"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn resolve_prefers_explicit_then_registry_then_defaults() {
        let explicit_dir = scratch("game-resolve-explicit");
        let explicit_exe = explicit_dir.join("GenshinImpact.exe");
        std::fs::write(&explicit_exe, b"stub").unwrap();

        // 显式路径优先级最高：即使注册表目录也可用，也要用显式的。
        let registry_dir = scratch("game-resolve-registry");
        let registry_exe = registry_dir.join("YuanShen.exe");
        std::fs::write(&registry_exe, b"stub").unwrap();
        let found = resolve(Some(&explicit_exe), vec![registry_dir.clone()]).unwrap();
        assert_eq!(found, explicit_exe);

        // 没有显式路径时用注册表目录。
        let found = resolve(None, vec![registry_dir.clone()]).unwrap();
        assert_eq!(found, registry_exe);

        // 注册表目录里没有游戏时继续找后面的目录。
        let empty = scratch("game-resolve-empty");
        let found = resolve(None, vec![empty.clone(), registry_dir.clone()]).unwrap();
        assert_eq!(found, registry_exe);

        // 全都没有时给出可操作的报错。
        let error = resolve(None, vec![empty.clone()]).expect_err("找不到游戏必须报错");
        let message = error.to_string();
        assert!(
            message.contains("--game-path"),
            "报错应提示 --game-path: {message}"
        );

        for dir in [explicit_dir, registry_dir, empty] {
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }
}
