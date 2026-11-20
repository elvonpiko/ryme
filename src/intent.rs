//! What this client means to do with the microphone, separate from whatever
//! the server last reported.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Aim {
    /// Connected, or about to be, but not in a room.
    Outside,
    /// The room the user asked for. Voice stays shut until this name is confirmed.
    Room(String),
}

/// True only when the confirmed room is the one this client asked for.
pub fn admits(aim: &Aim, channel: &str) -> bool {
    match aim {
        Aim::Room(want) => !want.is_empty() && want == channel,
        Aim::Outside => false,
    }
}

/// A ticket is usable only on the server that issued it.
pub fn ticket_for(home: [u8; 32], dest: [u8; 32], ticket: [u8; 32]) -> [u8; 32] {
    if home != [0; 32] && home == dest {
        ticket
    } else {
        [0; 32]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_stays_shut_until_the_asked_room_is_confirmed() {
        let lobby = Aim::Room("Lobby".into());
        let stage = Aim::Room("Stage".into());
        assert!(!admits(&Aim::Outside, "Lobby"));
        assert!(!admits(&stage, "Lobby"));
        assert!(!admits(&stage, ""));
        assert!(admits(&stage, "Stage"));
        assert!(admits(&lobby, "Lobby"));
    }

    #[test]
    fn a_ticket_does_not_follow_a_different_server() {
        let home = [1; 32];
        let other = [2; 32];
        let ticket = [9; 32];
        assert_eq!(ticket_for(home, home, ticket), ticket);
        assert_eq!(ticket_for(home, other, ticket), [0; 32]);
        assert_eq!(ticket_for([0; 32], [0; 32], ticket), [0; 32]);
    }
}
