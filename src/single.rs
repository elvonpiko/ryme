//! One Ryme process per desktop session.
//!
//! Closing the card hides it and leaves the call up. A second launch has to
//! reveal that window. Starting another process joins the lobby beside the
//! one that is already there.

use std::sync::Mutex;

use slint::{ComponentHandle, Weak};

static SHOW: Mutex<Option<Weak<crate::MainWindow>>> = Mutex::new(None);

pub fn watch(weak: Weak<crate::MainWindow>) {
    *SHOW.lock().unwrap() = Some(weak);
    #[cfg(unix)]
    spawn_listener();
}

pub fn show_now() {
    let weak = SHOW.lock().unwrap().clone();
    let Some(weak) = weak else {
        return;
    };
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = weak.upgrade() {
            ui.window().set_minimized(false);
            let _ = ui.window().show();
        }
    });
}

/// True when this process owns the session. False when another Ryme is already
/// running and has been asked to show its card.
pub fn primary() -> bool {
    #[cfg(windows)]
    {
        return win_primary();
    }
    #[cfg(unix)]
    {
        return unix_primary();
    }
    #[cfg(not(any(windows, unix)))]
    {
        true
    }
}

#[cfg(windows)]
pub fn arm() {
    if show_message() != 0 {
        return;
    }
    let name: Vec<u16> = "Ryme.Show\0".encode_utf16().collect();
    let msg = unsafe { RegisterWindowMessageW(name.as_ptr()) };
    if msg != 0 {
        SHOW_MSG.store(msg, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(windows)]
pub fn show_message() -> u32 {
    SHOW_MSG.load(std::sync::atomic::Ordering::Acquire)
}

#[cfg(windows)]
static SHOW_MSG: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cfg(windows)]
fn win_primary() -> bool {
    let name: Vec<u16> = "Local\\Ryme\0".encode_utf16().collect();
    let handle = unsafe { CreateMutexW(std::ptr::null(), 1, name.as_ptr()) };
    if handle.is_null() {
        return true;
    }
    if unsafe { GetLastError() } == 183 {
        unsafe { CloseHandle(handle) };
        raise_existing();
        return false;
    }
    // The raw handle stays open until this process exits. That is the lock.
    let _keep = handle;
    true
}

#[cfg(windows)]
fn raise_existing() {
    let title: Vec<u16> = "ryme\0".encode_utf16().collect();
    let message: Vec<u16> = "Ryme.Show\0".encode_utf16().collect();
    unsafe {
        let hwnd = FindWindowW(std::ptr::null(), title.as_ptr());
        if hwnd.is_null() {
            return;
        }
        let command = if IsIconic(hwnd) != 0 { 9 } else { 5 };
        ShowWindow(hwnd, command);
        SetForegroundWindow(hwnd);
        let msg = RegisterWindowMessageW(message.as_ptr());
        if msg != 0 {
            let mut result = 0usize;
            SendMessageTimeoutW(hwnd, msg, 0, 0, 0x0002, 500, &mut result);
        }
    }
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn CreateMutexW(
        attrs: *const std::ffi::c_void,
        owner: i32,
        name: *const u16,
    ) -> *mut std::ffi::c_void;
    fn GetLastError() -> u32;
    fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
}

#[cfg(windows)]
#[link(name = "user32")]
extern "system" {
    fn FindWindowW(class: *const u16, title: *const u16) -> *mut std::ffi::c_void;
    fn ShowWindow(hwnd: *mut std::ffi::c_void, command: i32) -> i32;
    fn SetForegroundWindow(hwnd: *mut std::ffi::c_void) -> i32;
    fn IsIconic(hwnd: *mut std::ffi::c_void) -> i32;
    fn RegisterWindowMessageW(name: *const u16) -> u32;
    fn SendMessageTimeoutW(
        hwnd: *mut std::ffi::c_void,
        msg: u32,
        wparam: usize,
        lparam: isize,
        flags: u32,
        timeout: u32,
        result: *mut usize,
    ) -> i32;
}

#[cfg(unix)]
static LISTENER: Mutex<Option<std::os::unix::net::UnixListener>> = Mutex::new(None);

#[cfg(unix)]
fn unix_primary() -> bool {
    let path = socket_path();
    if poke(&path) {
        return false;
    }
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&path);
    match std::os::unix::net::UnixListener::bind(&path) {
        Ok(listener) => {
            *LISTENER.lock().unwrap() = Some(listener);
            *SOCKET.lock().unwrap() = Some(SocketPath(path));
            true
        }
        Err(_) => !poke(&path),
    }
}

#[cfg(unix)]
fn poke(path: &std::path::Path) -> bool {
    std::os::unix::net::UnixStream::connect(path).is_ok()
}

#[cfg(unix)]
fn spawn_listener() {
    let listener = LISTENER.lock().unwrap().take();
    let Some(listener) = listener else {
        return;
    };
    std::thread::spawn(move || {
        for _conn in listener.incoming().flatten() {
            show_now();
        }
    });
}

#[cfg(unix)]
fn socket_path() -> std::path::PathBuf {
    let dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let user = std::env::var("USER").unwrap_or_else(|_| "user".into());
            std::path::PathBuf::from(format!("/tmp/ryme-{user}"))
        });
    dir.join("ryme.sock")
}

#[cfg(unix)]
static SOCKET: Mutex<Option<SocketPath>> = Mutex::new(None);

#[cfg(unix)]
struct SocketPath(std::path::PathBuf);

#[cfg(unix)]
impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
