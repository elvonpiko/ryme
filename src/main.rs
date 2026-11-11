#![cfg_attr(windows, windows_subsystem = "windows")]

slint::include_modules!();

mod ear;
mod jitter;
mod live;
mod local;

mod card_shape;

#[cfg(target_os = "linux")]
mod x11_frame;

#[cfg(windows)]
mod win_frame;

fn main() -> Result<(), slint::PlatformError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio");
    let ui = MainWindow::new()?;
    live::attach(&ui, rt.handle());

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

    #[cfg(windows)]
    win_frame::install(&ui);

    ui.run()
}
