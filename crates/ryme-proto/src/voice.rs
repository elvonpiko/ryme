use crate::ProtoError;

pub const VOICE_VERSION: u8 = 1;
/// version, speaker, sequence, timestamp, flags.
pub const VOICE_HEADER: usize = 14;
pub const MAX_VOICE_PAYLOAD: usize = 1000;
/// Reserved. Set when the Opus bytes are encrypted for the room, not the server.
pub const VOICE_ENCRYPTED: u8 = 0b0000_0001;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoicePacket {
    pub speaker: u32,
    pub seq: u32,
    pub timestamp: u32,
    pub flags: u8,
    pub payload: Vec<u8>,
}

pub fn encode_voice(packet: &VoicePacket) -> Result<Vec<u8>, ProtoError> {
    if packet.payload.len() > MAX_VOICE_PAYLOAD {
        return Err(ProtoError::TooLarge);
    }
    let mut out = Vec::with_capacity(VOICE_HEADER + packet.payload.len());
    out.push(VOICE_VERSION);
    out.extend_from_slice(&packet.speaker.to_le_bytes());
    out.extend_from_slice(&packet.seq.to_le_bytes());
    out.extend_from_slice(&packet.timestamp.to_le_bytes());
    out.push(packet.flags);
    out.extend_from_slice(&packet.payload);
    Ok(out)
}

pub fn decode_voice(buf: &[u8]) -> Result<VoicePacket, ProtoError> {
    check_header(buf)?;
    let speaker = u32::from_le_bytes(buf[1..5].try_into().unwrap());
    let seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
    let timestamp = u32::from_le_bytes(buf[9..13].try_into().unwrap());
    Ok(VoicePacket {
        speaker,
        seq,
        timestamp,
        flags: buf[13],
        payload: buf[VOICE_HEADER..].to_vec(),
    })
}

/// The connection already knows who spoke. Overwrite whatever the client wrote.
pub fn stamp_speaker(buf: &mut [u8], speaker: u32) -> Result<(), ProtoError> {
    check_header(buf)?;
    buf[1..5].copy_from_slice(&speaker.to_le_bytes());
    Ok(())
}

fn check_header(buf: &[u8]) -> Result<(), ProtoError> {
    if buf.len() < VOICE_HEADER {
        return Err(ProtoError::Truncated);
    }
    if buf[0] != VOICE_VERSION {
        return Err(ProtoError::Version);
    }
    if buf.len() - VOICE_HEADER > MAX_VOICE_PAYLOAD {
        return Err(ProtoError::TooLarge);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VoicePacket {
        VoicePacket {
            speaker: 4,
            seq: 7,
            timestamp: 960,
            flags: 0,
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
            speaker: 0,
            seq: 0,
            timestamp: 0,
            flags: 0,
            payload: vec![],
        };
        assert_eq!(
            decode_voice(&encode_voice(&packet).unwrap()).unwrap(),
            packet
        );
    }

    #[test]
    fn seq_and_timestamp_keep_high_bits() {
        let packet = VoicePacket {
            speaker: u32::MAX,
            seq: u32::MAX,
            timestamp: u32::MAX - 1,
            flags: 0xff,
            payload: vec![9],
        };
        assert_eq!(
            decode_voice(&encode_voice(&packet).unwrap()).unwrap(),
            packet
        );
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
            speaker: 1,
            seq: 1,
            timestamp: 1,
            flags: 0,
            payload: vec![0; MAX_VOICE_PAYLOAD + 1],
        };
        assert_eq!(encode_voice(&packet).unwrap_err(), ProtoError::TooLarge);

        let mut bytes = encode_voice(&VoicePacket {
            speaker: 1,
            seq: 1,
            timestamp: 1,
            flags: 0,
            payload: vec![0; 4],
        })
        .unwrap();
        bytes.extend(std::iter::repeat(0).take(MAX_VOICE_PAYLOAD));
        assert_eq!(decode_voice(&bytes).unwrap_err(), ProtoError::TooLarge);
    }

    #[test]
    fn the_server_overwrites_a_claimed_speaker() {
        let mut bytes = encode_voice(&sample()).unwrap();
        stamp_speaker(&mut bytes, 1).unwrap();
        let heard = decode_voice(&bytes).unwrap();
        assert_eq!(heard.speaker, 1);
        assert_eq!(heard.seq, 7);
        assert_eq!(heard.payload, vec![1, 2, 3, 4]);
        assert!(stamp_speaker(&mut bytes[..4], 1).is_err());
    }
}
