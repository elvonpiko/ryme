//! Open the card in the middle of the work area. Wayland chooses the
//! position itself, so this is a no-op there.

use std::sync::atomic::{AtomicBool, Ordering};

use slint::ComponentHandle;

static PLACED: AtomicBool = AtomicBool::new(false);

pub fn install(ui: &crate::MainWindow) {
    center(ui);
    for delay in [40_u64, 200] {
        let weak = ui.as_weak();
        slint::Timer::single_shot(std::time::Duration::from_millis(delay), move || {
            if let Some(ui) = weak.upgrade() {
                center(&ui);
            }
        });
    }
}

fn center(ui: &crate::MainWindow) {
    if PLACED.load(Ordering::Relaxed) {
        return;
    }
    let window = ui.window();
    let size = window.size();
    if size.width <= 1 || size.height <= 1 {
        return;
    }
    let Some((x, y)) = work_center(&window, size.width, size.height) else {
        return;
    };
    window.set_position(slint::PhysicalPosition::new(x, y));
    PLACED.store(true, Ordering::Relaxed);
}

#[cfg(windows)]
fn work_center(window: &slint::Window, width: u32, height: u32) -> Option<(i32, i32)> {
    use raw_window_handle::{HasWindowHandle, RawWindowHandle};
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::Graphics::Gdi::{
        GetMonitorInfoW, MonitorFromWindow, MONITORINFO, MONITOR_DEFAULTTONEAREST,
    };

    let handle = window.window_handle();
    let raw = HasWindowHandle::window_handle(&handle).ok()?.as_raw();
    let hwnd = match raw {
        RawWindowHandle::Win32(h) => h.hwnd.get() as HWND,
        _ => return None,
    };
    unsafe {
        let monitor = MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST);
        if monitor.is_null() {
            return None;
        }
        let mut info: MONITORINFO = std::mem::zeroed();
        info.cbSize = std::mem::size_of::<MONITORINFO>() as u32;
        if GetMonitorInfoW(monitor, &mut info) == 0 {
            return None;
        }
        Some(center_in(info.rcWork, width, height))
    }
}

#[cfg(windows)]
fn center_in(work: windows_sys::Win32::Foundation::RECT, width: u32, height: u32) -> (i32, i32) {
    let x = work.left + (work.right - work.left - width as i32) / 2;
    let y = work.top + (work.bottom - work.top - height as i32) / 2;
    (x, y)
}

#[cfg(target_os = "linux")]
fn work_center(window: &slint::Window, width: u32, height: u32) -> Option<(i32, i32)> {
    let _ = window;
    let (conn, screen_num) = x11rb::connect(None).ok()?;
    let (root, screen_w, screen_h) = {
        let screen = conn.setup().roots.get(screen_num)?;
        (screen.root, screen.width_in_pixels, screen.height_in_pixels)
    };
    let (x, y, w, h) =
        work_area(&conn, root).unwrap_or((0, 0, screen_w as i32, screen_h as i32));
    Some((x + (w - width as i32) / 2, y + (h - height as i32) / 2))
}

#[cfg(target_os = "linux")]
fn work_area(
    conn: &x11rb::rust_connection::RustConnection,
    root: u32,
) -> Option<(i32, i32, i32, i32)> {
    use x11rb::protocol::xproto::{AtomEnum, ConnectionExt};
    let atom = conn.intern_atom(false, b"_NET_WORKAREA").ok()?.reply().ok()?.atom;
    let reply = conn
        .get_property(false, root, atom, AtomEnum::CARDINAL, 0, 4)
        .ok()?
        .reply()
        .ok()?;
    let values = reply.value32()?;
    let values: Vec<u32> = values.collect();
    if values.len() < 4 {
        return None;
    }
    Some((
        values[0] as i32,
        values[1] as i32,
        values[2] as i32,
        values[3] as i32,
    ))
}

#[cfg(target_os = "macos")]
fn work_center(_window: &slint::Window, width: u32, height: u32) -> Option<(i32, i32)> {
    #[repr(C)]
    struct Rect {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    }
    #[link(name = "CoreGraphics", kind = "framework")]
    extern "C" {
        fn CGMainDisplayID() -> u32;
        fn CGDisplayBounds(display: u32) -> Rect;
    }
    let bounds = unsafe { CGDisplayBounds(CGMainDisplayID()) };
    if bounds.width < 1.0 || bounds.height < 1.0 {
        return None;
    }
    let x = bounds.x + (bounds.width - width as f64) / 2.0;
    let y = bounds.y + (bounds.height - height as f64) / 2.0;
    Some((x.round() as i32, y.round() as i32))
}

#[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
fn work_center(_window: &slint::Window, _width: u32, _height: u32) -> Option<(i32, i32)> {
    None
}
