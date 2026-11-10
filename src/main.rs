slint::include_modules!();

#[cfg(target_os = "linux")]
mod x11_frame;

fn main() -> Result<(), slint::PlatformError> {
    let ui = MainWindow::new()?;

    ui.on_request_close(|| {
        let _ = slint::quit_event_loop();
    });

    let weak = ui.as_weak();
    ui.on_minimize(move || {
        if let Some(ui) = weak.upgrade() {
            ui.window().set_minimized(true);
        }
    });

    // Frameless window dragging (works on Windows and X11).
    // Wayland forbids apps from positioning themselves; see notes.
    let weak = ui.as_weak();
    ui.on_drag(move |dx, dy| {
        if let Some(ui) = weak.upgrade() {
            let w = ui.window();
            let s = w.scale_factor();
            let p = w.position();
            w.set_position(slint::PhysicalPosition::new(
                p.x + (dx * s) as i32,
                p.y + (dy * s) as i32,
            ));
        }
    });

    #[cfg(target_os = "linux")]
    x11_frame::install(&ui);

    ui.run()
}
