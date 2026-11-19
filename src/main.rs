#![cfg_attr(windows, windows_subsystem = "windows")]

slint::include_modules!();

mod ear;
mod jitter;
mod live;
mod local;
mod update;

mod card_shape;
mod place;
mod single;

#[cfg(target_os = "linux")]
mod linux_tray;
#[cfg(target_os = "linux")]
mod x11_frame;

#[cfg(windows)]
mod win_frame;

fn main() -> Result<(), slint::PlatformError> {
    if let Some(invite) = probe_arg() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio");
        rt.block_on(run_probe(&invite));
        return Ok(());
    }
    if let Some(url) = std::env::args().find(|arg| arg.starts_with("ryme://")) {
        local::stage_invite(&url);
    }
    let handoff = std::env::args().any(|arg| arg == "--handoff");
    if !single::claim(handoff) {
        return Ok(());
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio");
    let ui = MainWindow::new()?;
    // The dock matches this id to ryme.desktop. It has to be set before the
    // window is created, which is the first time the card is shown.
    let _ = slint::set_xdg_app_id("ryme");
    let shutdown = live::attach(&ui, rt.handle());
    single::watch(ui.as_weak());

    let weak = ui.as_weak();
    ui.on_minimize(move || {
        if let Some(ui) = weak.upgrade() {
            ui.window().set_minimized(true);
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

    // Closing the card hides it. The tray is what keeps the process, and the
    // call, alive after that.
    ui.window()
        .on_close_requested(|| slint::CloseRequestResponse::HideWindow);

    // Slint's own tray registers as "slint-tray". GNOME treats that as a
    // second application, so the dock shows two windows. The Linux tray uses
    // the desktop id instead. Other desktops keep Slint's tray.
    #[cfg(target_os = "linux")]
    {
        return match linux_tray::install(shutdown) {
            Ok(()) => slint::run_event_loop_until_quit(),
            Err(shutdown) => keep_in_tray(&ui, shutdown),
        };
    }
    #[cfg(not(target_os = "linux"))]
    keep_in_tray(&ui, shutdown)
}

fn probe_arg() -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--probe" {
            return args.next();
        }
        if let Some(invite) = arg.strip_prefix("--probe=") {
            return Some(invite.to_string());
        }
    }
    None
}

async fn run_probe(invite: &str) {
    #[cfg(windows)]
    show_probe_console();
    println!("client {}", env!("CARGO_PKG_VERSION"));
    match ryme_server::probe_invite(invite).await {
        Ok(reports) => {
            if reports.is_empty() {
                println!("no address");
                return;
            }
            for report in reports {
                println!(
                    "{} {} {} {}ms",
                    report.transport, report.address, report.outcome, report.millis
                );
            }
        }
        Err(err) => println!("invite {err:?}"),
    }
}

#[cfg(windows)]
fn show_probe_console() {
    use windows_sys::Win32::System::Console::{AllocConsole, AttachConsole, ATTACH_PARENT_PROCESS};
    unsafe {
        if AttachConsole(ATTACH_PARENT_PROCESS) == 0 {
            AllocConsole();
        }
    }
}

fn keep_in_tray(ui: &MainWindow, shutdown: live::Shutdown) -> Result<(), slint::PlatformError> {
    let tray = RymeTray::new()?;
    let weak = ui.as_weak();
    tray.on_show_window(move || {
        if let Some(ui) = weak.upgrade() {
            ui.window().set_minimized(false);
            let _ = ui.window().show();
        }
    });
    tray.on_quit(move || {
        if shutdown.hang_up() {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        let _ = slint::quit_event_loop();
    });
    tray.show()?;
    let tray_weak = tray.as_weak();
    let ui_weak = ui.as_weak();
    let timer = slint::Timer::default();
    timer.start(
        slint::TimerMode::Repeated,
        std::time::Duration::from_millis(400),
        move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let Some(tray) = tray_weak.upgrade() else {
                return;
            };
            tray.set_mic_status(ui.get_mic_line());
        },
    );
    std::mem::forget(timer);
    ui.run()
}
