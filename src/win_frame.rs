//! Clip the frameless card so the corners outside the fill are not black.
//!
//! Windows paints a transparent Slint window as an opaque rectangle. The same
//! torn outline used on X11 becomes the window region, and Windows 11 is told
//! not to add its own border or rounded frame on top of that.

use std::ffi::c_void;
use std::sync::atomic::{AtomicIsize, Ordering};

use raw_window_handle::{HasWindowHandle, RawWindowHandle};
use slint::ComponentHandle;
use windows_sys::Win32::Foundation::{HWND, LPARAM, LRESULT, POINT, WPARAM};
use windows_sys::Win32::Graphics::Dwm::{
    DwmSetWindowAttribute, DWMNCRP_DISABLED, DWMWA_BORDER_COLOR, DWMWA_COLOR_NONE,
    DWMWA_NCRENDERING_POLICY, DWMWA_WINDOW_CORNER_PREFERENCE, DWMWCP_DONOTROUND,
};
use windows_sys::Win32::Graphics::Gdi::{CreatePolygonRgn, DeleteObject, SetWindowRgn, WINDING};

use crate::card_shape::outline_px;

pub fn install(ui: &crate::MainWindow) {
    crate::single::arm();
    polish(ui);
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
        // The window style still has a caption. Desktop Window Manager paints
        // that caption whenever the card is not the active window, including
        // the taskbar preview and a double-click on the tray icon.
        let policy = DWMNCRP_DISABLED;
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_NCRENDERING_POLICY as u32,
            &policy as *const i32 as *const c_void,
            std::mem::size_of_val(&policy) as u32,
        );
    }
    keep_caption_off(hwnd);
}

/// `WM_NCACTIVATE` with `lParam == -1` updates the active state and does not
/// paint the system title bar. The other messages are the themed caption.
const WM_NCPAINT: u32 = 0x0085;
const WM_NCACTIVATE: u32 = 0x0086;
const WM_NCUAHDRAWCAPTION: u32 = 0x00AE;
const WM_NCUAHDRAWFRAME: u32 = 0x00AF;
const GWLP_WNDPROC: i32 = -4;

#[link(name = "user32")]
extern "system" {
    fn SetWindowLongPtrW(hwnd: HWND, index: i32, new_long: isize) -> isize;
    fn CallWindowProcW(
        prev: isize,
        hwnd: HWND,
        msg: u32,
        wparam: WPARAM,
        lparam: LPARAM,
    ) -> LRESULT;
    fn SetWindowPos(
        hwnd: HWND,
        insert_after: HWND,
        x: i32,
        y: i32,
        cx: i32,
        cy: i32,
        flags: u32,
    ) -> i32;
}

static PREV_WNDPROC: AtomicIsize = AtomicIsize::new(0);

fn keep_caption_off(hwnd: HWND) {
    if PREV_WNDPROC.load(Ordering::Acquire) != 0 {
        return;
    }
    let prev = unsafe { SetWindowLongPtrW(hwnd, GWLP_WNDPROC, wnd_proc as isize) };
    if prev == 0 {
        return;
    }
    PREV_WNDPROC.store(prev, Ordering::Release);
    // Ask for a fresh non-client size now that the caption paint is blocked.
    unsafe {
        SetWindowPos(
            hwnd,
            std::ptr::null_mut(),
            0,
            0,
            0,
            0,
            0x0001 | 0x0002 | 0x0004 | 0x0010 | 0x0020,
        );
    }
}

unsafe extern "system" fn wnd_proc(
    hwnd: HWND,
    msg: u32,
    wparam: WPARAM,
    lparam: LPARAM,
) -> LRESULT {
    let prev = PREV_WNDPROC.load(Ordering::Acquire);
    if prev == 0 {
        return 0;
    }
    if msg != 0 && msg == crate::single::show_message() {
        crate::single::show_now();
        return 0;
    }
    match msg {
        WM_NCPAINT | WM_NCUAHDRAWCAPTION | WM_NCUAHDRAWFRAME => 0,
        WM_NCACTIVATE => CallWindowProcW(prev, hwnd, msg, wparam, -1),
        _ => CallWindowProcW(prev, hwnd, msg, wparam, lparam),
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
