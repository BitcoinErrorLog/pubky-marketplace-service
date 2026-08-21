/// The z-base-32 alphabet used by Pubky identifiers.
pub const Z_BASE_32_ALPHABET: &str = "ybndrfg8ejkmcpqxot1uwisza345h769";

/// A Pubky is exactly 52 z-base-32 characters (an encoded ed25519 public key).
pub fn is_valid_pubky(value: &str) -> bool {
    value.len() == 52 && value.chars().all(|c| Z_BASE_32_ALPHABET.contains(c))
}

/// Encodes a 32-byte ed25519 public key as its 52-character z-base-32 pubky.
pub fn encode_pubky(key: &[u8; 32]) -> String {
    let alphabet = Z_BASE_32_ALPHABET.as_bytes();
    let mut accumulator: u64 = 0;
    let mut bit_count: u32 = 0;
    let mut output = String::with_capacity(52);
    for &byte in key {
        accumulator = (accumulator << 8) | u64::from(byte);
        bit_count += 8;
        while bit_count >= 5 {
            bit_count -= 5;
            output.push(alphabet[((accumulator >> bit_count) & 31) as usize] as char);
        }
    }
    // 256 bits leave a 1-bit remainder, padded low to a final 5-bit group.
    output.push(alphabet[((accumulator << (5 - bit_count)) & 31) as usize] as char);
    output
}

#[cfg(test)]
mod tests {
    use super::{encode_pubky, is_valid_pubky};

    #[test]
    fn encodes_keys_as_valid_pubkys() {
        assert!(is_valid_pubky(&encode_pubky(&[0u8; 32])));
        assert!(is_valid_pubky(&encode_pubky(&[0xFF; 32])));
        assert_eq!(encode_pubky(&[0u8; 32]), "y".repeat(52));
        let mut counting = [0u8; 32];
        for (index, byte) in counting.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let encoded = encode_pubky(&counting);
        assert_eq!(encoded.len(), 52);
        assert!(is_valid_pubky(&encoded));
    }

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
