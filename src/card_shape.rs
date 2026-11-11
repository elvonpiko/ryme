//! The torn card in `ui/app.slint`. Linux and Windows both clip the window
//! to this outline, so the corners outside the fill are not black rectangles.

pub const CARD_W: f32 = 384.0;
pub const CARD_H: f32 = 290.0;

/// Flattened fill path, in the 384×290 viewBox. The closing point is omitted.
pub fn outline() -> Vec<(f32, f32)> {
    // M 2 34
    // C 2 14 16 2 38 2
    // L 382 2 L 382 266 L 374 272 L 380 278 L 372 284 L 376 288 L 2 288 L 2 34
    let mut pts = vec![(2.0_f32, 34.0)];
    cubic(&mut pts, (2.0, 14.0), (16.0, 2.0), (38.0, 2.0));
    for p in [
        (382.0, 2.0),
        (382.0, 266.0),
        (374.0, 272.0),
        (380.0, 278.0),
        (372.0, 284.0),
        (376.0, 288.0),
        (2.0, 288.0),
        (2.0, 34.0),
    ] {
        pts.push(p);
    }
    if pts.len() > 1 {
        let first = pts[0];
        if let Some(last) = pts.last().copied() {
            if (last.0 - first.0).abs() < 0.05 && (last.1 - first.1).abs() < 0.05 {
                pts.pop();
            }
        }
    }
    pts
}

/// Scale the viewBox outline into the window's physical pixels.
pub fn outline_px(px_w: u32, px_h: u32) -> Vec<(f32, f32)> {
    let sx = px_w as f32 / CARD_W;
    let sy = px_h as f32 / CARD_H;
    outline()
        .into_iter()
        .map(|(x, y)| (x * sx, y * sy))
        .collect()
}

fn cubic(pts: &mut Vec<(f32, f32)>, c1: (f32, f32), c2: (f32, f32), to: (f32, f32)) {
    let p0 = *pts.last().unwrap();
    const N: i32 = 24;
    for i in 1..=N {
        let t = i as f32 / N as f32;
        let u = 1.0 - t;
        let x =
            u * u * u * p0.0 + 3.0 * u * u * t * c1.0 + 3.0 * u * t * t * c2.0 + t * t * t * to.0;
        let y =
            u * u * u * p0.1 + 3.0 * u * u * t * c1.1 + 3.0 * u * t * t * c2.1 + t * t * t * to.1;
        pts.push((x, y));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_card_outline_is_the_torn_fill() {
        let pts = outline();
        assert!(pts.len() > 20);
        assert!((pts[0].0 - 2.0).abs() < 0.01);
        assert!((pts[0].1 - 34.0).abs() < 0.01);
        let last = *pts.last().unwrap();
        assert!((last.0 - 2.0).abs() < 0.01);
        assert!((last.1 - 34.0).abs() > 1.0);
    }
}
