//! Inlined data-dir resolution (tracer copy of `apps/core/src/paths.rs`, matching
//! `apps-store/quests/backend/src/paths.rs` and `apps-store/mail/backend/src/paths.rs`).
//!
//! The sidecar MUST resolve the SAME data dir Core uses so it opens the SAME
//! `monitors.db`. The load-bearing rule is `RYU_DIR`-env-first: Core/Kernel passes
//! `RYU_DIR` to the sidecar at spawn, guaranteeing co-location. The pointer-file
//! read + `RYU_PROFILE` suffix are replicated for faithfulness in the headless
//! case, but env-first + default is what actually guarantees the shared path.

use std::path::PathBuf;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

const RYU_DIR_ENV: &str = "RYU_DIR";
const RYU_PROFILE_ENV: &str = "RYU_PROFILE";
const RELEASE_PROFILE: &str = "release";

/// Data-dir / config-dir suffix for the active profile: `""` for release,
/// `-<profile>` otherwise (e.g. `-dev`). Mirrors `crate::profile::suffix`.
fn suffix() -> String {
    let profile = std::env::var(RYU_PROFILE_ENV)
        .ok()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| RELEASE_PROFILE.to_string());
    if profile == RELEASE_PROFILE {
        String::new()
    } else {
        format!("-{}", profile.trim())
    }
}

/// The default data dir: `~/.ryu{suffix}` (falling back to `./.ryu` if home is
/// unknown).
fn default_ryu_dir() -> PathBuf {
    let name = format!(".ryu{}", suffix());
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(name)
}

/// Config dir holding the bootstrap pointer file (`ryu{suffix}` under the OS
/// config dir), NOT inside the data dir.
fn config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(default_ryu_dir)
        .join(format!("ryu{}", suffix()))
}

fn pointer_path() -> PathBuf {
    config_dir().join("data-path.json")
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct DataPathPointer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_dir: Option<String>,
}

fn read_pointer() -> DataPathPointer {
    let Ok(bytes) = std::fs::read(pointer_path()) else {
        return DataPathPointer::default();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn resolve() -> PathBuf {
    if let Some(v) = std::env::var_os(RYU_DIR_ENV) {
        let p = PathBuf::from(v);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    if let Some(dir) = read_pointer().data_dir {
        let p = PathBuf::from(dir);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    default_ryu_dir()
}

static RYU_DIR: OnceLock<PathBuf> = OnceLock::new();

/// The active data dir, resolved once and cached for the process lifetime.
pub fn ryu_dir() -> PathBuf {
    RYU_DIR.get_or_init(resolve).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    // These assertions are env-read-only (they never mutate process env, which
    // would be a cross-thread data race under the parallel test runner). They
    // exercise the default-resolution path, which is what env-first guarantees.

    #[test]
    fn default_ryu_dir_and_config_dir_are_named() {
        // suffix() is "" under the release profile (the test env default).
        let data = default_ryu_dir();
        assert!(data
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .starts_with(".ryu"));

        let cfg = config_dir();
        assert!(cfg
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .starts_with("ryu"));
    }

    #[test]
    fn pointer_path_lives_under_config_dir() {
        let p = pointer_path();
        assert_eq!(p.file_name().and_then(|n| n.to_str()), Some("data-path.json"));
        assert_eq!(p.parent(), Some(config_dir().as_path()));
    }

    #[test]
    fn read_pointer_defaults_when_absent_or_unparseable() {
        // The pointer file almost certainly does not exist in a test sandbox, so
        // the read falls back to the default (empty) pointer.
        let ptr = read_pointer();
        assert!(ptr.data_dir.is_none());
    }

    #[test]
    fn resolve_and_ryu_dir_return_a_path() {
        // With no RYU_DIR env and no pointer file, resolve() lands on the default
        // dir; ryu_dir() caches whatever resolve() produced.
        let resolved = resolve();
        assert!(!resolved.as_os_str().is_empty());
        assert!(!ryu_dir().as_os_str().is_empty());
    }

    #[test]
    fn data_path_pointer_deserializes_from_json() {
        let ptr: DataPathPointer =
            serde_json::from_str(r#"{"data_dir":"/tmp/custom"}"#).unwrap();
        assert_eq!(ptr.data_dir.as_deref(), Some("/tmp/custom"));
        // Round trips (the skip-if-none keeps a null out).
        let empty = DataPathPointer::default();
        assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");
    }
}
