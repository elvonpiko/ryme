//! Clip the frameless card so the corners outside the fill are not black.
//!
//! Windows paints a transparent Slint window as an opaque rectangle. The same
//! torn outline used on X11 becomes the window region, and Windows 11 is told
//! not to add its own border or rounded frame on top of that.

use std::ffi::c_void;

use raw_window_handle::{HasWindowHandle, RawWindowHandle};
use slint::ComponentHandle;
use windows_sys::Win32::Foundation::{HWND, POINT};
use windows_sys::Win32::Graphics::Dwm::{
    DwmSetWindowAttribute, DWMWA_BORDER_COLOR, DWMWA_COLOR_NONE, DWMWA_WINDOW_CORNER_PREFERENCE,
    DWMWCP_DONOTROUND,
};
use windows_sys::Win32::Graphics::Gdi::{CreatePolygonRgn, DeleteObject, SetWindowRgn, WINDING};

use crate::card_shape::outline_px;

pub fn install(ui: &crate::MainWindow) {
    for delay in [80_u64, 250, 700, 1400] {
        let weak = ui.as_weak();
        slint::Timer::single_shot(std::time::Duration::from_millis(delay), move || {
            if let Some(ui) = weak.upgrade() {
                polish(&ui);
            }
        });
    }
}

fn polish(ui: &crate::MainWindow) {
    let window = ui.window();
    let Some(hwnd) = hwnd(window) else {
        return;
    };
    drop_system_frame(hwnd);

    let size = window.size();
    if size.width > 1 && size.height > 1 {
        clip(hwnd, size.width, size.height);
    }
}

fn hwnd(window: &slint::Window) -> Option<HWND> {
    let handle = window.window_handle();
    let raw = HasWindowHandle::window_handle(&handle).ok()?.as_raw();
    match raw {
        RawWindowHandle::Win32(h) => Some(h.hwnd.get() as HWND),
        _ => None,
    }
}

fn drop_system_frame(hwnd: HWND) {
    unsafe {
        let pref = DWMWCP_DONOTROUND;
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_WINDOW_CORNER_PREFERENCE as u32,
            &pref as *const i32 as *const c_void,
            std::mem::size_of_val(&pref) as u32,
        );
        let color = DWMWA_COLOR_NONE;
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_BORDER_COLOR as u32,
            &color as *const u32 as *const c_void,
            std::mem::size_of_val(&color) as u32,
        );
    }
}

fn clip(hwnd: HWND, px_w: u32, px_h: u32) {
    let points: Vec<POINT> = outline_px(px_w, px_h)
        .into_iter()
        .map(|(x, y)| POINT {
            x: x.round() as i32,
            y: y.round() as i32,
        })
        .collect();
    if points.len() < 3 {
        return;
    }

    unsafe {
        let region = CreatePolygonRgn(points.as_ptr(), points.len() as i32, WINDING);
        if region.is_null() {
            warn_once("could not build the card region");
            return;
        }
        // A successful call takes ownership of the region.
        if SetWindowRgn(hwnd, region, 1) == 0 {
            DeleteObject(region);
            warn_once("could not clip the window");
        }
    }
}

fn warn_once(err: &str) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static ONCE: AtomicBool = AtomicBool::new(false);
    if !ONCE.swap(true, Ordering::Relaxed) {
        eprintln!("ryme: {err}");
    }
}
