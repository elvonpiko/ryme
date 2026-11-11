#![cfg_attr(windows, windows_subsystem = "windows")]

slint::include_modules!();

mod ear;
mod jitter;
mod live;
mod local;
mod update;

mod card_shape;
mod place;

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

    // Show before the first paint is pumped, so the card can be centered
    // and the system caption can be turned off while it is still unseen.
    let _ = ui.show();

    #[cfg(target_os = "linux")]
    x11_frame::install(&ui);

    #[cfg(windows)]
    win_frame::install(&ui);

    place::install(&ui);

    let tray = RymeTray::new()?;
    let weak = ui.as_weak();
    tray.on_show_window(move || {
        if let Some(ui) = weak.upgrade() {
            ui.window().set_minimized(false);
            let _ = ui.window().show();
        }
    });
    tray.on_quit(|| {
        let _ = slint::quit_event_loop();
    });
    tray.show()?;

    // The tray keeps the process alive, so closing the card only hides it.
    ui.window().on_close_requested(|| slint::CloseRequestResponse::HideWindow);

    ui.run()
}
