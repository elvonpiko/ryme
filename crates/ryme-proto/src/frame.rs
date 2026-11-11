use serde::{de::DeserializeOwned, Serialize};

use crate::control::{normalize_client, normalize_server, ClientMsg, ServerMsg};
use crate::ProtoError;

pub const MAX_FRAME: usize = 4096;
const CONTROL_VERSION: u8 = 1;

pub fn encode_client(msg: &ClientMsg) -> Result<Vec<u8>, ProtoError> {
    encode_frame(msg)
}

pub fn encode_server(msg: &ServerMsg) -> Result<Vec<u8>, ProtoError> {
    encode_frame(msg)
}

pub fn decode_client_frame(buf: &[u8]) -> Result<Option<(ClientMsg, usize)>, ProtoError> {
    match decode_frame(buf)? {
        Some((msg, n)) => Ok(Some((normalize_client(msg)?, n))),
        None => Ok(None),
    }
}

pub fn decode_server_frame(buf: &[u8]) -> Result<Option<(ServerMsg, usize)>, ProtoError> {
    match decode_frame(buf)? {
        Some((msg, n)) => Ok(Some((normalize_server(msg)?, n))),
        None => Ok(None),
    }
}

fn encode_frame<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtoError> {
    let body = postcard::to_allocvec(msg).map_err(|_| ProtoError::Codec)?;
    let len = 1 + body.len();
    if len > MAX_FRAME {
        return Err(ProtoError::TooLarge);
    }
    let mut out = Vec::with_capacity(4 + len);
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.push(CONTROL_VERSION);
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode_frame<T: DeserializeOwned>(buf: &[u8]) -> Result<Option<(T, usize)>, ProtoError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME || len == 0 {
        return Err(ProtoError::TooLarge);
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    if buf[4] != CONTROL_VERSION {
        return Err(ProtoError::Version);
    }
    let msg = postcard::from_bytes(&buf[5..4 + len]).map_err(|_| ProtoError::Codec)?;
    Ok(Some((msg, 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ClientMsg;

    #[test]
    fn one_frame_roundtrips() {
        let frame = encode_client(&ClientMsg::Leave).unwrap();
        let (msg, n) = decode_client_frame(&frame).unwrap().unwrap();
        assert_eq!(n, frame.len());
        assert_eq!(msg, ClientMsg::Leave);
    }

    #[test]
    fn two_frames_in_one_buffer() {
        let hello = encode_client(&ClientMsg::Hello {
            name: "Mara".into(),
        })
        .unwrap();
        let join = encode_client(&ClientMsg::Join {
            channel: "Lobby".into(),
        })
        .unwrap();
        let mut buf = hello.clone();
        buf.extend_from_slice(&join);

        let (first, n) = decode_client_frame(&buf).unwrap().unwrap();
        assert_eq!(
            first,
            ClientMsg::Hello {
                name: "Mara".into()
            }
        );
        let (second, m) = decode_client_frame(&buf[n..]).unwrap().unwrap();
        assert_eq!(
            second,
            ClientMsg::Join {
                channel: "Lobby".into()
            }
        );
        assert_eq!(n + m, buf.len());
    }

    #[test]
    fn partial_frame_waits() {
        let frame = encode_client(&ClientMsg::Leave).unwrap();
        assert!(decode_client_frame(&frame[..3]).unwrap().is_none());
        assert!(decode_client_frame(&frame[..frame.len() - 1])
            .unwrap()
            .is_none());
    }

    #[test]
    fn huge_length_is_rejected_without_the_body() {
        let mut buf = (1_000_000u32).to_le_bytes().to_vec();
        buf.push(CONTROL_VERSION);
        assert_eq!(decode_client_frame(&buf).unwrap_err(), ProtoError::TooLarge);
    }

    #[test]
    fn bad_version_and_garbage_are_rejected() {
        let mut frame = encode_client(&ClientMsg::Leave).unwrap();
        frame[4] = 9;
        assert_eq!(
            decode_client_frame(&frame).unwrap_err(),
            ProtoError::Version
        );

        let garbage = [4, 0, 0, 0, CONTROL_VERSION, 0xff, 0xff, 0xff];
        assert_eq!(
            decode_client_frame(&garbage).unwrap_err(),
            ProtoError::Codec
        );
    }

    #[test]
    fn hello_on_the_wire_trims_the_name() {
        let frame = encode_client(&ClientMsg::Hello {
            name: "  Ravi ".into(),
        })
        .unwrap();
        let (msg, _) = decode_client_frame(&frame).unwrap().unwrap();
        assert_eq!(
            msg,
            ClientMsg::Hello {
                name: "Ravi".into()
            }
        );
    }

    #[test]
    fn invalid_name_fails_after_a_well_formed_frame() {
        let frame = encode_client(&ClientMsg::Hello {
            name: " \n ".into(),
        })
        .unwrap();
        assert_eq!(
            decode_client_frame(&frame).unwrap_err(),
            ProtoError::Invalid
        );
    }
}
