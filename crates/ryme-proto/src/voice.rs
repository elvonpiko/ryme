use crate::ProtoError;

pub const VOICE_VERSION: u8 = 1;
pub const VOICE_HEADER: usize = 10;
pub const MAX_VOICE_PAYLOAD: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoicePacket {
    pub flags: u8,
    pub seq: u32,
    pub timestamp: u32,
    pub payload: Vec<u8>,
}

pub fn encode_voice(packet: &VoicePacket) -> Result<Vec<u8>, ProtoError> {
    if packet.payload.len() > MAX_VOICE_PAYLOAD {
        return Err(ProtoError::TooLarge);
    }
    let mut out = Vec::with_capacity(VOICE_HEADER + packet.payload.len());
    out.push(VOICE_VERSION);
    out.push(packet.flags);
    out.extend_from_slice(&packet.seq.to_le_bytes());
    out.extend_from_slice(&packet.timestamp.to_le_bytes());
    out.extend_from_slice(&packet.payload);
    Ok(out)
}

pub fn decode_voice(buf: &[u8]) -> Result<VoicePacket, ProtoError> {
    if buf.len() < VOICE_HEADER {
        return Err(ProtoError::Truncated);
    }
    if buf[0] != VOICE_VERSION {
        return Err(ProtoError::Version);
    }
    if buf.len() - VOICE_HEADER > MAX_VOICE_PAYLOAD {
        return Err(ProtoError::TooLarge);
    }
    let seq = u32::from_le_bytes(buf[2..6].try_into().unwrap());
    let timestamp = u32::from_le_bytes(buf[6..10].try_into().unwrap());
    Ok(VoicePacket {
        flags: buf[1],
        seq,
        timestamp,
        payload: buf[VOICE_HEADER..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VoicePacket {
        VoicePacket {
            flags: 0b0000_0001,
            seq: 7,
            timestamp: 960,
            payload: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn roundtrip() {
        let packet = sample();
        let bytes = encode_voice(&packet).unwrap();
        assert_eq!(bytes.len(), VOICE_HEADER + 4);
        assert_eq!(decode_voice(&bytes).unwrap(), packet);
    }

    #[test]
    fn empty_payload_is_silence() {
        let packet = VoicePacket {
            flags: 0,
            seq: 0,
            timestamp: 0,
            payload: vec![],
        };
        assert_eq!(decode_voice(&encode_voice(&packet).unwrap()).unwrap(), packet);
    }

    #[test]
    fn seq_and_timestamp_keep_high_bits() {
        let packet = VoicePacket {
            flags: 0xff,
            seq: u32::MAX,
            timestamp: u32::MAX - 1,
            payload: vec![9],
        };
        assert_eq!(decode_voice(&encode_voice(&packet).unwrap()).unwrap(), packet);
    }

    #[test]
    fn short_buffer_is_truncated() {
        assert_eq!(decode_voice(&[1, 2, 3]).unwrap_err(), ProtoError::Truncated);
        assert_eq!(decode_voice(&[]).unwrap_err(), ProtoError::Truncated);
    }

    #[test]
    fn wrong_version_is_rejected() {
        let mut bytes = encode_voice(&sample()).unwrap();
        bytes[0] = 2;
        assert_eq!(decode_voice(&bytes).unwrap_err(), ProtoError::Version);
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let packet = VoicePacket {
            flags: 0,
            seq: 1,
            timestamp: 1,
            payload: vec![0; MAX_VOICE_PAYLOAD + 1],
        };
        assert_eq!(encode_voice(&packet).unwrap_err(), ProtoError::TooLarge);

        let mut bytes = encode_voice(&VoicePacket {
            flags: 0,
            seq: 1,
            timestamp: 1,
            payload: vec![0; 4],
        })
        .unwrap();
        bytes.extend(std::iter::repeat(0).take(MAX_VOICE_PAYLOAD));
        assert_eq!(decode_voice(&bytes).unwrap_err(), ProtoError::TooLarge);
    }
}
