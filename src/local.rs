use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct Saved {
    pub name: String,
    pub invite: String,
    pub channel: String,
    pub resume: bool,
    pub denoise: bool,
}

#[derive(Clone, Copy)]
pub struct Level {
    pub gain: f32,
    pub quiet: bool,
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
    let denoise = lines.next().map(|line| line != "0").unwrap_or(true);
    if name.is_empty() || invite.is_empty() || name.contains('\n') || invite.contains('\n') {
        return None;
    }
    Some(Saved {
        name,
        invite,
        channel,
        resume,
        denoise,
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
        "v1\n{}\n{}\n{}\n{}\n{}\n",
        saved.name,
        saved.invite,
        saved.channel,
        if saved.resume { "1" } else { "0" },
        if saved.denoise { "1" } else { "0" }
    );
    let _ = write_private(&path, body.as_bytes());
}

fn path() -> Option<PathBuf> {
    Some(dir()?.join("session"))
}

/// This install's stable id. Other people store a volume against it.
pub fn card() -> u64 {
    if let Some(card) = read_card() {
        return card;
    }
    let card = fresh_card();
    write_card(card);
    card
}

fn read_card() -> Option<u64> {
    let text = fs::read_to_string(dir()?.join("card")).ok()?;
    let card = u64::from_str_radix(text.trim(), 16).ok()?;
    (card != 0).then_some(card)
}

fn write_card(card: u64) {
    let Some(path) = dir().map(|dir| dir.join("card")) else {
        return;
    };
    let _ = write_private(&path, format!("{card:016x}\n").as_bytes());
}

fn fresh_card() -> u64 {
    let mut bytes = [0u8; 8];
    fill_random(&mut bytes);
    let card = u64::from_le_bytes(bytes);
    if card == 0 {
        1
    } else {
        card
    }
}

fn fill_random(bytes: &mut [u8]) {
    #[cfg(unix)]
    {
        use std::io::Read;
        if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
            if file.read_exact(bytes).is_ok() {
                return;
            }
        }
    }
    #[cfg(windows)]
    {
        #[link(name = "advapi32")]
        extern "system" {
            fn SystemFunction036(buf: *mut u8, len: u32) -> u8;
        }
        let ok = unsafe { SystemFunction036(bytes.as_mut_ptr(), bytes.len() as u32) };
        if ok != 0 {
            return;
        }
    }
    let ticks = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = ticks
            .rotate_left((index as u32) * 7)
            .wrapping_add(index as u64) as u8;
    }
}

pub fn load_levels(server: &[u8; 32]) -> HashMap<u64, Level> {
    let Some(text) = dir().and_then(|dir| fs::read_to_string(dir.join("levels")).ok()) else {
        return HashMap::new();
    };
    let server = hex_encode(server);
    let mut levels = HashMap::new();
    for line in text.lines().skip(1) {
        let mut parts = line.split(' ');
        let Some(fp) = parts.next() else { continue };
        if fp != server {
            continue;
        }
        let Some(card) = parts
            .next()
            .and_then(|word| u64::from_str_radix(word, 16).ok())
        else {
            continue;
        };
        let Some(gain) = parts.next().and_then(|word| word.parse::<i32>().ok()) else {
            continue;
        };
        let quiet = parts.next() == Some("1");
        levels.insert(
            card,
            Level {
                gain: (gain as f32 / 1000.0).clamp(0.0, 2.0),
                quiet,
            },
        );
    }
    levels
}

pub fn store_levels(server: &[u8; 32], levels: &HashMap<u64, Level>) {
    let Some(path) = dir().map(|dir| dir.join("levels")) else {
        return;
    };
    let mut kept = Vec::new();
    if let Ok(text) = fs::read_to_string(&path) {
        let server_hex = hex_encode(server);
        for line in text.lines().skip(1) {
            if line.split(' ').next() != Some(server_hex.as_str()) && !line.is_empty() {
                kept.push(line.to_string());
            }
        }
    }
    let mut body = String::from("v1\n");
    for line in kept {
        body.push_str(&line);
        body.push('\n');
    }
    let server_hex = hex_encode(server);
    let mut rows: Vec<_> = levels.iter().collect();
    rows.sort_by_key(|(card, _)| *card);
    for (card, level) in rows {
        let gain = (level.gain.clamp(0.0, 2.0) * 1000.0).round() as i32;
        body.push_str(&format!(
            "{server_hex} {card:016x} {gain} {}\n",
            if level.quiet { "1" } else { "0" }
        ));
    }
    let _ = write_private(&path, body.as_bytes());
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn dir() -> Option<PathBuf> {
    let mut base = data_home()?;
    base.push("ryme");
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
        }
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
