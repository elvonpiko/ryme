fn main() {
    slint_build::compile("ui/app.slint").unwrap();

    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("packaging/icon/ryme.ico");
        res.compile().expect("embed the Ryme icon");
    }
}
