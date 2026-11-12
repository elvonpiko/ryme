//! Tray icon whose id is the desktop file id.
//!
//! Slint's tray always registers as "slint-tray". GNOME's indicator extension
//! turns that into a second dock entry. This icon is "ryme", and it is a
//! system service so it stays in the top bar instead of the window list.

use std::sync::Mutex;

use ksni::blocking::TrayMethods;
use ksni::menu::StandardItem;

static STOP: Mutex<Option<crate::live::Shutdown>> = Mutex::new(None);

struct LinuxTray;

impl ksni::Tray for LinuxTray {
    fn id(&self) -> String {
        "ryme".into()
    }

    fn category(&self) -> ksni::Category {
        ksni::Category::SystemServices
    }

    fn icon_name(&self) -> String {
        "ryme".into()
    }

    fn title(&self) -> String {
        "Ryme".into()
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip {
            title: "Ryme".into(),
            ..Default::default()
        }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        crate::single::show_now();
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        vec![
            StandardItem {
                label: "Show".into(),
                activate: Box::new(|_| crate::single::show_now()),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Quit".into(),
                activate: Box::new(|_| quit()),
                ..Default::default()
            }
            .into(),
        ]
    }
}

fn quit() {
    let stop = STOP.lock().unwrap().take();
    if let Some(stop) = stop {
        if stop.hang_up() {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
    let _ = slint::quit_event_loop();
}

/// `Ok` when the icon is up. The shutdown handle comes back when the desktop
/// has no status notifier, so the caller can fall back to another tray.
pub fn install(shutdown: crate::live::Shutdown) -> Result<(), crate::live::Shutdown> {
    *STOP.lock().unwrap() = Some(shutdown);
    match LinuxTray.spawn() {
        Ok(handle) => {
            Box::leak(Box::new(handle));
            Ok(())
        }
        Err(_) => Err(STOP.lock().unwrap().take().expect("shutdown")),
    }
}
