//! One TCP connection carries both paths. Each frame is a tag, a length, and
//! the bytes the other path already uses. Voice is not reliable: the sender
//! drops a frame rather than wait for a stalled socket.

use crate::{ProtoError, MAX_FRAME};

pub const TCP_CONTROL: u8 = 1;
pub const TCP_VOICE: u8 = 2;
/// A control frame is a 4-byte length plus at most MAX_FRAME bytes.
pub const TCP_MAX: usize = 4 + MAX_FRAME;

pub fn encode_tagged(tag: u8, payload: &[u8]) -> Result<Vec<u8>, ProtoError> {
    if payload.len() > TCP_MAX {
        return Err(ProtoError::TooLarge);
    }
    let mut out = Vec::with_capacity(3 + payload.len());
    out.push(tag);
    out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// `None` means the buffer does not hold a whole frame yet.
pub fn decode_tagged(buf: &[u8]) -> Result<Option<(u8, &[u8], usize)>, ProtoError> {
    if buf.len() < 3 {
        return Ok(None);
    }
    let len = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;
    if len > TCP_MAX {
        return Err(ProtoError::TooLarge);
    }
    if buf.len() < 3 + len {
        return Ok(None);
    }
    if buf[0] != TCP_CONTROL && buf[0] != TCP_VOICE {
        return Err(ProtoError::Invalid);
    }
    Ok(Some((buf[0], &buf[3..3 + len], 3 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_roundtrips_and_a_partial_one_waits() {
        let frame = encode_tagged(TCP_VOICE, &[9, 8, 7]).unwrap();
        assert!(decode_tagged(&frame[..2]).unwrap().is_none());
        let (tag, payload, n) = decode_tagged(&frame).unwrap().unwrap();
        assert_eq!(tag, TCP_VOICE);
        assert_eq!(payload, &[9, 8, 7]);
        assert_eq!(n, frame.len());
    }

    #[test]
    fn two_frames_and_a_bad_tag() {
        let mut buf = encode_tagged(TCP_CONTROL, &[1]).unwrap();
        buf.extend_from_slice(&encode_tagged(TCP_VOICE, &[2, 3]).unwrap());
        let (tag, payload, n) = decode_tagged(&buf).unwrap().unwrap();
        assert_eq!((tag, payload), (TCP_CONTROL, &[1][..]));
        let (tag, payload, m) = decode_tagged(&buf[n..]).unwrap().unwrap();
        assert_eq!((tag, payload), (TCP_VOICE, &[2, 3][..]));
        assert_eq!(n + m, buf.len());

        let junk = [9, 1, 0, 0];
        assert_eq!(decode_tagged(&junk).unwrap_err(), ProtoError::Invalid);
        let huge = [TCP_VOICE, 0xff, 0xff];
        assert_eq!(decode_tagged(&huge).unwrap_err(), ProtoError::TooLarge);
    }
}
