use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct Saved {
    pub name: String,
    pub invite: String,
    pub channel: String,
    pub resume: bool,
}

pub fn load() -> Option<Saved> {
    let text = fs::read_to_string(path()?).ok()?;
    let mut lines = text.lines();
    if lines.next()? != "v1" {
        return None;
    }
    let name = lines.next()?.to_string();
    let invite = lines.next()?.to_string();
    let channel = lines.next().unwrap_or("Lobby").to_string();
    let resume = lines.next()? == "1";
    if name.is_empty() || invite.is_empty() || name.contains('\n') || invite.contains('\n') {
        return None;
    }
    Some(Saved {
        name,
        invite,
        channel,
        resume,
    })
}

pub fn store(saved: &Saved) {
    let Some(path) = path() else {
        return;
    };
    if let Some(dir) = path.parent() {
        let _ = fs::create_dir_all(dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
        }
    }
    let body = format!(
        "v1\n{}\n{}\n{}\n{}\n",
        saved.name,
        saved.invite,
        saved.channel,
        if saved.resume { "1" } else { "0" }
    );
    let _ = write_private(&path, body.as_bytes());
}

fn path() -> Option<PathBuf> {
    let mut base = data_home()?;
    base.push("ryme");
    base.push("session");
    Some(base)
}

fn data_home() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        return std::env::var_os("APPDATA").map(PathBuf::from);
    }
    #[cfg(not(windows))]
    {
        if let Some(dir) = std::env::var_os("XDG_DATA_HOME") {
            return Some(PathBuf::from(dir));
        }
        let mut home = PathBuf::from(std::env::var_os("HOME")?);
        home.push(".local");
        home.push("share");
        Some(home)
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = temp_path(path);
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}
