//! A settings check against the GitHub release. A newer build is downloaded,
//! checked against SHA256SUMS, and applied when the user asks.
//!
//! Asset names match .github/workflows/release.yml.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use sha2::{Digest, Sha256};

const CURRENT: &str = env!("CARGO_PKG_VERSION");
const LATEST: &str = "https://api.github.com/repos/elvonpiko/ryme/releases/latest";

#[derive(Clone, Copy)]
enum Kind {
    WindowsSetup,
    Replace,
    Deb,
}

struct Ready {
    version: String,
    path: PathBuf,
    kind: Kind,
}

static READY: Mutex<Option<Ready>> = Mutex::new(None);

pub enum Found {
    Current,
    Ready(String),
    Failed(String),
}

/// Look up the latest release. Downloads and checks it when a newer one exists.
pub fn check() -> Found {
    if let Some(version) = READY
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|ready| ready.version.clone()))
    {
        return Found::Ready(version);
    }
    let Some((name, kind)) = asset() else {
        return Found::Failed("No update is published for this system.".into());
    };
    let release = match fetch_latest() {
        Ok(release) => release,
        Err(_) => return Found::Failed("Could not check for an update.".into()),
    };
    if !is_newer(&release.tag, CURRENT) {
        return Found::Current;
    }
    let Some(url) = release.assets.get(name).cloned() else {
        return Found::Failed("No update is published for this system.".into());
    };
    if !allowed(&url) {
        return Found::Failed("Could not check for an update.".into());
    }
    let dir = std::env::temp_dir().join("ryme-update");
    if fs::create_dir_all(&dir).is_err() {
        return Found::Failed("Could not save the update.".into());
    }
    let path = dir.join(name);
    if download(&url, &path).is_err() {
        return Found::Failed("Could not download the update.".into());
    }
    let sums_url = format!(
        "https://github.com/elvonpiko/ryme/releases/download/{}/SHA256SUMS",
        release.tag
    );
    let sums_path = dir.join("SHA256SUMS");
    if download(&sums_url, &sums_path).is_err() {
        return Found::Failed("Could not check the update.".into());
    }
    let Ok(sums) = fs::read_to_string(&sums_path) else {
        return Found::Failed("Could not check the update.".into());
    };
    let Some(want) = sum_for(&sums, name) else {
        let _ = fs::remove_file(&path);
        return Found::Failed("Could not check the update.".into());
    };
    let Ok(got) = sha256_file(&path) else {
        return Found::Failed("Could not check the update.".into());
    };
    if !want.eq_ignore_ascii_case(&got) {
        let _ = fs::remove_file(&path);
        return Found::Failed("The update did not match.".into());
    }
    let version = release.tag.trim().trim_start_matches('v').to_string();
    let Ok(mut guard) = READY.lock() else {
        return Found::Failed("Could not save the update.".into());
    };
    *guard = Some(Ready {
        version: version.clone(),
        path,
        kind,
    });
    Found::Ready(version)
}

pub fn launch(rt: tokio::runtime::Handle, on_error: impl FnOnce(String) + Send + 'static) {
    let ready = READY.lock().expect("update").take();
    let Some(ready) = ready else {
        return;
    };
    let restore = ready.clone_ready();
    rt.spawn(async move {
        let result = tokio::task::spawn_blocking(move || install(&ready)).await;
        let failed = match result {
            Ok(Ok(())) => None,
            Ok(Err(message)) => Some(message),
            Err(_) => Some("Could not apply the update.".into()),
        };
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(message) = failed {
                *READY.lock().expect("update") = Some(restore);
                on_error(message);
            } else {
                let _ = slint::quit_event_loop();
            }
        });
    });
}

struct Release {
    tag: String,
    assets: std::collections::HashMap<String, String>,
}

fn fetch_latest() -> Result<Release, String> {
    let response = agent(Duration::from_secs(20))
        .get(LATEST)
        .set("User-Agent", "ryme")
        .set("Accept", "application/vnd.github+json")
        .call()
        .map_err(|_| "no release".to_string())?;
    let value: serde_json::Value = response.into_json().map_err(|_| "no release".to_string())?;
    let tag = value
        .get("tag_name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if tag.is_empty() {
        return Err("no release".into());
    }
    let mut assets = std::collections::HashMap::new();
    if let Some(list) = value.get("assets").and_then(|v| v.as_array()) {
        for item in list {
            let Some(name) = item.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(url) = item.get("browser_download_url").and_then(|v| v.as_str()) else {
                continue;
            };
            assets.insert(name.to_string(), url.to_string());
        }
    }
    Ok(Release { tag, assets })
}

fn download(url: &str, dest: &Path) -> Result<(), String> {
    if !allowed(url) {
        return Err("unexpected download".into());
    }
    let response = agent(Duration::from_secs(180))
        .get(url)
        .set("User-Agent", "ryme")
        .call()
        .map_err(|_| "download failed".to_string())?;
    let partial = dest.with_extension("partial");
    let mut file = File::create(&partial).map_err(|_| "could not save the update".to_string())?;
    io::copy(&mut response.into_reader(), &mut file).map_err(|_| "download failed".to_string())?;
    file.flush().ok();
    drop(file);
    fs::rename(&partial, dest).map_err(|_| "could not save the update".to_string())?;
    Ok(())
}

fn install(ready: &Ready) -> Result<(), String> {
    match ready.kind {
        Kind::WindowsSetup => install_setup(&ready.path),
        Kind::Replace => install_replace(&ready.path),
        Kind::Deb => install_deb(&ready.path),
    }
}

fn install_setup(path: &Path) -> Result<(), String> {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED: u32 = 0x0000_0008 | 0x0000_0200 | 0x0100_0000;
        std::process::Command::new(path)
            .args(["/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART"])
            .creation_flags(DETACHED)
            .spawn()
            .map_err(|_| "could not start the installer".to_string())?;
        return Ok(());
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        Err("could not start the installer".into())
    }
}

fn install_replace(new_file: &Path) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|_| "could not find Ryme".to_string())?;
    let staged = exe.with_extension("new");
    fs::copy(new_file, &staged).map_err(|_| "could not save the update".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))
            .map_err(|_| "could not save the update".to_string())?;
    }
    fs::rename(&staged, &exe).map_err(|_| "could not replace Ryme".to_string())?;
    std::process::Command::new(&exe)
        .spawn()
        .map_err(|_| "could not restart Ryme".to_string())?;
    Ok(())
}

fn install_deb(path: &Path) -> Result<(), String> {
    let status = std::process::Command::new("pkexec")
        .args(["dpkg", "-i"])
        .arg(path)
        .status()
        .map_err(|_| "could not start the installer".to_string())?;
    if !status.success() {
        return Err("The update was not installed.".into());
    }
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("ryme"));
    std::process::Command::new(exe)
        .spawn()
        .map_err(|_| "Ryme was updated. Open it again.".to_string())?;
    Ok(())
}

fn asset() -> Option<(&'static str, Kind)> {
    #[cfg(all(windows, target_arch = "x86_64"))]
    return Some(("Ryme-Setup-x64.exe", Kind::WindowsSetup));
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        if can_replace_exe() {
            return Some(("ryme-x86_64-unknown-linux-gnu", Kind::Replace));
        }
        return Some(("ryme-amd64.deb", Kind::Deb));
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    return Some(("ryme-aarch64-apple-darwin", Kind::Replace));
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    return Some(("ryme-x86_64-apple-darwin", Kind::Replace));
    #[cfg(not(any(
        all(windows, target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
    )))]
    return None;
}

#[cfg(target_os = "linux")]
fn can_replace_exe() -> bool {
    let Ok(exe) = std::env::current_exe() else {
        return false;
    };
    let Some(dir) = exe.parent() else {
        return false;
    };
    let probe = dir.join(".ryme-update-probe");
    match File::create(&probe) {
        Ok(_) => {
            let _ = fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

fn allowed(url: &str) -> bool {
    url.starts_with("https://github.com/elvonpiko/ryme/releases/download/")
        || url.starts_with("https://release-assets.githubusercontent.com/")
        || url.starts_with("https://objects.githubusercontent.com/")
}

fn agent(read: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(15))
        .timeout_read(read)
        .timeout_write(Duration::from_secs(30))
        .build()
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sum_for<'a>(sums: &'a str, name: &str) -> Option<&'a str> {
    for line in sums.lines() {
        let mut parts = line.split_whitespace();
        let hash = parts.next()?;
        let file = parts.next()?;
        if file == name {
            return Some(hash);
        }
    }
    None
}

pub fn is_newer(latest: &str, current: &str) -> bool {
    fn parse(value: &str) -> Option<[u64; 3]> {
        let value = value.trim().trim_start_matches('v');
        let value = value.split('-').next().unwrap_or(value);
        let mut parts = value.split('.');
        Some([
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
            parts.next().unwrap_or("0").parse().ok()?,
        ])
    }
    match (parse(latest), parse(current)) {
        (Some(latest), Some(current)) => latest > current,
        _ => false,
    }
}

impl Ready {
    fn clone_ready(&self) -> Ready {
        Ready {
            version: self.version.clone(),
            path: self.path.clone(),
            kind: self.kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{allowed, is_newer, sum_for};

    #[test]
    fn a_newer_tag_is_newer() {
        assert!(is_newer("v0.1.2", "0.1.1"));
        assert!(is_newer("0.2.0", "0.1.9"));
        assert!(!is_newer("0.1.1", "0.1.1"));
        assert!(!is_newer("v0.1.0", "0.1.1"));
    }

    #[test]
    fn sums_match_the_release_file() {
        let sums = "abc  ryme-amd64.deb\ndef  Ryme-Setup-x64.exe\n";
        assert_eq!(sum_for(sums, "Ryme-Setup-x64.exe"), Some("def"));
        assert_eq!(sum_for(sums, "missing"), None);
    }

    #[test]
    fn downloads_stay_on_the_release() {
        assert!(allowed(
            "https://github.com/elvonpiko/ryme/releases/download/v0.1.2/Ryme-Setup-x64.exe"
        ));
        assert!(!allowed("https://example.com/Ryme-Setup-x64.exe"));
    }
}
