/// IEEE CRC32 used to compare private output with its expected bytes after EIO.
/// Keep the polynomial, initial state and final XOR compatible with the existing
/// partition CRCs: deferred one-shot recovery still supplies their combined CRC.
///
/// Only select the wider implementation on CPUs covered by the performance
/// comparison. Other CPUs retain crc32fast's existing dispatch and fallback.
// Only retain the running CRC. Digest also stores large, constant algorithm
// parameters; moving those with every IPC writer needlessly enlarges its state.
pub(super) enum RecoveryChecksum {
    Wide(u32),
    Portable(crc32fast::Hasher),
}

impl RecoveryChecksum {
    pub(super) fn new() -> Self {
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("sse4.1")
            && std::is_x86_feature_detected!("pclmulqdq")
            && std::is_x86_feature_detected!("avx512vl")
            && std::is_x86_feature_detected!("vpclmulqdq")
        {
            return Self::Wide(u32::MAX);
        }
        Self::Portable(crc32fast::Hasher::new())
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Wide(state) => {
                let mut hasher = crc_fast::Digest::new_with_init_state(
                    crc_fast::CrcAlgorithm::Crc32IsoHdlc,
                    u64::from(*state),
                );
                hasher.update(bytes);
                *state = hasher.get_state() as u32;
            }
            Self::Portable(hasher) => hasher.update(bytes),
        }
    }

    pub(super) fn finalize(&self) -> u32 {
        match self {
            // This variant is always CRC-32/ISO-HDLC, never CRC32C or CRC64.
            Self::Wide(state) => !state,
            Self::Portable(hasher) => hasher.clone().finalize(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RecoveryChecksum;

    #[test]
    fn ieee_crc_matches_existing_partition_checksums_across_update_boundaries() {
        let bytes: Vec<_> = (0..4 * 1024 * 1024 + 96)
            .map(|i: usize| (i.wrapping_mul(31) ^ (i >> 7) ^ (i >> 13)) as u8)
            .collect();
        let mut known = RecoveryChecksum::new();
        known.update(b"123456789");
        assert_eq!(known.finalize(), 0xcbf43926);

        for offset in [0, 1, 15, 31, 63] {
            for len in [
                0,
                1,
                63,
                64,
                127,
                128,
                4095,
                4096,
                8191,
                8192,
                8193,
                4 * 1024 * 1024 + 7,
            ] {
                let value = &bytes[offset..offset + len];
                let expected = crc32fast::hash(value);
                for chunk_size in [1, 63, 8192, 4 * 1024 * 1024] {
                    if len > 8193 && chunk_size < 8192 {
                        continue;
                    }
                    for mut actual in [
                        RecoveryChecksum::new(),
                        RecoveryChecksum::Portable(crc32fast::Hasher::new()),
                    ] {
                        actual.update(&[]);
                        for chunk in value.chunks(chunk_size) {
                            actual.update(chunk);
                        }
                        assert_eq!(actual.finalize(), expected, "{offset}/{len}/{chunk_size}");
                    }
                }
            }
        }
    }
}
