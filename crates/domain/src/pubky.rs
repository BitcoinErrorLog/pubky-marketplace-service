/// The z-base-32 alphabet used by Pubky identifiers.
pub const Z_BASE_32_ALPHABET: &str = "ybndrfg8ejkmcpqxot1uwisza345h769";

/// A Pubky is exactly 52 z-base-32 characters (an encoded ed25519 public key).
pub fn is_valid_pubky(value: &str) -> bool {
    value.len() == 52 && value.chars().all(|c| Z_BASE_32_ALPHABET.contains(c))
}

#[cfg(test)]
mod tests {
    use super::is_valid_pubky;

    #[test]
    fn accepts_z_base_32_pubkys() {
        assert!(is_valid_pubky(&"y".repeat(52)));
        assert!(is_valid_pubky(&"o".repeat(52)));
    }

    #[test]
    fn rejects_wrong_length_and_alphabet() {
        assert!(!is_valid_pubky(&"y".repeat(51)));
        assert!(!is_valid_pubky(&"y".repeat(53)));
        assert!(!is_valid_pubky(&"l".repeat(52)));
        assert!(!is_valid_pubky(&"v".repeat(52)));
        assert!(!is_valid_pubky(&"Y".repeat(52)));
        assert!(!is_valid_pubky("not-a-pubky"));
    }
}
