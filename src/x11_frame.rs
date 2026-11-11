//! Clip the frameless card so the cut corners are not black rectangles.
//!
//! This session is i3 without a compositor. A transparent window is still a
//! rectangle, and the pixels outside the card are painted black. i3 also
//! tiles that rectangle and draws its own frame. The outline is `card_shape`,
//! the same path drawn in `ui/app.slint`.

use raw_window_handle::{HasWindowHandle, RawWindowHandle};
use slint::ComponentHandle;
use x11rb::connection::Connection;
use x11rb::protocol::shape::{self, SK, SO};
use x11rb::protocol::xproto::ClipOrdering;
use x11rb::protocol::xproto::Rectangle;

use crate::card_shape::{outline_px, CARD_H, CARD_W};

pub fn install(ui: &crate::MainWindow) {
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
    let Some(xid) = x11_window_id(window) else {
        return;
    };

    let scale = window.scale_factor().max(1.0);
    let target_w = (CARD_W * scale).round().max(1.0) as u32;
    let target_h = (CARD_H * scale).round().max(1.0) as u32;
    float_card_on_i3(xid, target_w, target_h);

    let size = window.size();
    let off = (size.width as i32 - target_w as i32).abs() > 2
        || (size.height as i32 - target_h as i32).abs() > 2;
    if off {
        window.set_size(slint::PhysicalSize::new(target_w, target_h));
    }

    // Clip whatever size the window has right now. A later tick clips again
    // once i3 has finished the resize, so a stretched frame doesn't stick.
    if size.width > 1 && size.height > 1 {
        if let Err(err) = clip_to_card(xid, size.width, size.height) {
            warn_once(&err);
        }
    }
}

fn x11_window_id(window: &slint::Window) -> Option<u32> {
    let handle = window.window_handle();
    let raw = HasWindowHandle::window_handle(&handle).ok()?.as_raw();
    match raw {
        RawWindowHandle::Xlib(h) => u32::try_from(h.window).ok(),
        RawWindowHandle::Xcb(h) => Some(h.window.get()),
        _ => None,
    }
}

fn float_card_on_i3(xid: u32, width: u32, height: u32) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static MOVED: AtomicBool = AtomicBool::new(false);
    // The first time the card floats, put it in the middle of the output.
    // Later ticks only keep the size, so a drag is not pulled back.
    let center = if MOVED.swap(true, Ordering::Relaxed) {
        ""
    } else {
        ", move position center"
    };
    let message = format!(
        "[id=\"{xid}\"] floating enable, border none, resize set {width} px {height} px{center}"
    );
    let ok = std::process::Command::new("i3-msg")
        .args(["-q", &message])
        .status()
        .is_ok_and(|status| status.success());
    if !ok {
        MOVED.store(false, Ordering::Relaxed);
    }
}

fn clip_to_card(xid: u32, px_w: u32, px_h: u32) -> Result<(), String> {
    let rects = card_rects(px_w, px_h);
    if rects.len() < 8 {
        return Err("card outline produced no shape".into());
    }

    let (conn, _) = x11rb::connect(None).map_err(|err| err.to_string())?;
    for kind in [SK::BOUNDING, SK::INPUT] {
        shape::rectangles(
            &conn,
            SO::SET,
            kind,
            ClipOrdering::YX_BANDED,
            xid,
            0,
            0,
            &rects,
        )
        .map_err(|err| err.to_string())?
        .check()
        .map_err(|err| err.to_string())?;
    }
    conn.flush().map_err(|err| err.to_string())?;
    Ok(())
}

fn warn_once(err: &str) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static ONCE: AtomicBool = AtomicBool::new(false);
    if !ONCE.swap(true, Ordering::Relaxed) {
        eprintln!("ryme: could not clip the window shape ({err})");
    }
}

/// The card outline, scan-converted for the X shape extension.
fn card_rects(px_w: u32, px_h: u32) -> Vec<Rectangle> {
    scan_polygon(&outline_px(px_w, px_h), px_w as i32, px_h as i32)
}

fn scan_polygon(poly: &[(f32, f32)], width: i32, height: i32) -> Vec<Rectangle> {
    let mut rects = Vec::with_capacity(height as usize);
    if poly.len() < 3 || width <= 0 || height <= 0 {
        return rects;
    }
    let n = poly.len();
    for y in 0..height {
        let yf = y as f32 + 0.5;
        let mut xs = Vec::new();
        for i in 0..n {
            let p = poly[i];
            let q = poly[(i + 1) % n];
            let crosses = (p.1 <= yf && q.1 > yf) || (q.1 <= yf && p.1 > yf);
            if !crosses {
                continue;
            }
            let t = (yf - p.1) / (q.1 - p.1);
            xs.push(p.0 + t * (q.0 - p.0));
        }
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        for pair in xs.chunks_exact(2) {
            let mut x0 = pair[0].floor() as i32;
            let mut x1 = pair[1].ceil() as i32;
            if x0 < 0 {
                x0 = 0;
            }
            if x1 > width {
                x1 = width;
            }
            if x1 <= x0 {
                continue;
            }
            rects.push(Rectangle {
                x: x0 as i16,
                y: y as i16,
                width: (x1 - x0) as u16,
                height: 1,
            });
        }
    }
    rects
}
