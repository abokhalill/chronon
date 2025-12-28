use std::mem;

/// Header size is exactly 80 bytes (8-byte aligned).
/// Includes timestamp_ns for deterministic replay.
pub const HEADER_SIZE: usize = 80;

/// Maximum payload size per recovery.md: 64 MB.
pub const MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Genesis hash: all zeros, used as prev_hash for entry 0.
/// Per recovery.md II: "Genesis Hash: A fixed 256-bit value of all zeros"
pub const GENESIS_HASH: [u8; 16] = [0u8; 16];

/// Entry flags per log_format.md.
#[allow(dead_code)]
pub mod flags {
    pub const CONFIG_CHANGE: u16 = 0x01;
    pub const TOMBSTONE: u16 = 0x02;
    pub const CHECKPOINT: u16 = 0x04;
    /// End-of-log sentinel marker. Written after the last entry to detect silent truncation.
    pub const END_OF_LOG: u16 = 0x8000;
}

/// End-of-log sentinel size (16 bytes, aligned).
/// Format v2: [magic: 4 bytes][version: 1 byte][reserved: 3 bytes][last_index: 8 bytes]
/// Magic = 0x454F4C02 ("EOL" + version 2)
/// 
/// FORBIDDEN STATE F7: Sentinel index wrap.
/// Previous v1 format used u32 for last_index, which wraps at 2^32 entries.
/// v2 format uses full u64 to prevent silent wrap.
pub const SENTINEL_SIZE: usize = 16;

/// End-of-log sentinel magic bytes (includes version in last byte).
/// Version 1: 0x454F4C00 ("EOL\0") - 8 bytes, u32 index - DEPRECATED
/// Version 2: 0x454F4C02 ("EOL\x02") - 16 bytes, u64 index - CURRENT
pub const SENTINEL_MAGIC: [u8; 4] = [0x45, 0x4F, 0x4C, 0x02];

/// Legacy v1 sentinel magic for backward compatibility during recovery.
pub const SENTINEL_MAGIC_V1: [u8; 4] = [0x45, 0x4F, 0x4C, 0x00];

/// Create an end-of-log sentinel for the given last index.
/// Uses v2 format with full u64 index to prevent F7 (sentinel wrap).
#[allow(dead_code)]
pub fn create_sentinel(last_index: u64) -> [u8; SENTINEL_SIZE] {
    let mut sentinel = [0u8; SENTINEL_SIZE];
    // Bytes 0-3: Magic with version
    sentinel[0..4].copy_from_slice(&SENTINEL_MAGIC);
    // Bytes 4-7: Reserved (zero)
    // Bytes 8-15: Full u64 last_index (FIXES F7: no truncation)
    sentinel[8..16].copy_from_slice(&last_index.to_le_bytes());
    sentinel
}

/// Verify an end-of-log sentinel matches the expected last index.
/// Supports both v1 (legacy) and v2 formats for forward compatibility.
#[allow(dead_code)]
pub fn verify_sentinel(sentinel: &[u8; SENTINEL_SIZE], expected_last_index: u64) -> bool {
    // Check for v2 format first
    if sentinel[0..4] == SENTINEL_MAGIC {
        let stored_index = u64::from_le_bytes([
            sentinel[8], sentinel[9], sentinel[10], sentinel[11],
            sentinel[12], sentinel[13], sentinel[14], sentinel[15],
        ]);
        return stored_index == expected_last_index;
    }
    // Check for legacy v1 format (8-byte sentinel, u32 index)
    // Only valid if expected_last_index fits in u32
    if sentinel[0..4] == SENTINEL_MAGIC_V1 && expected_last_index <= u32::MAX as u64 {
        let stored_index = u32::from_le_bytes([sentinel[4], sentinel[5], sentinel[6], sentinel[7]]);
        return stored_index as u64 == expected_last_index;
    }
    false
}

/// Check if bytes represent a sentinel (v1 or v2).
#[allow(dead_code)]
pub fn is_sentinel_magic(bytes: &[u8]) -> bool {
    if bytes.len() < 4 {
        return false;
    }
    bytes[0..4] == SENTINEL_MAGIC || bytes[0..4] == SENTINEL_MAGIC_V1
}

/// The 80-byte log entry header.
///
/// # Layout
/// This struct uses #[repr(C)] but we serialize/deserialize manually to ensure
/// exact byte-level control matching the spec's offset table.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LogHeader {
    /// CRC32C of bytes [4..79] (Castagnoli polynomial).
    pub header_checksum: u32,
    /// Size of payload in bytes.
    pub payload_size: u32,
    /// Monotonic log index (0, 1, 2, ...).
    pub index: u64,
    /// Consensus view/term ID.
    pub view_id: u64,
    /// Routing hint for multi-tenancy.
    pub stream_id: u64,
    /// Truncated BLAKE3 hash of previous entry (16 bytes at offset 32).
    pub prev_hash: [u8; 16],
    /// Truncated BLAKE3 hash of this entry's payload (16 bytes at offset 48).
    pub payload_hash: [u8; 16],
    /// Consensus timestamp in nanoseconds since Unix epoch (offset 64).
    /// Assigned by Primary, agreed upon by quorum. Used for deterministic BlockTime.
    pub timestamp_ns: u64,
    /// Entry type bitmask (offset 72).
    pub flags: u16,
    /// Payload schema version (offset 74).
    pub schema_version: u16,
    /// Reserved for future use (offset 76).
    pub reserved: u32,
}

// Compile-time assertion: header must be exactly 80 bytes.
const _: () = assert!(mem::size_of::<LogHeader>() == HEADER_SIZE);

impl LogHeader {
    /// Create a new header with the given fields.
    /// The header_checksum is NOT computed here; call compute_checksum() after.
    pub fn new(
        index: u64,
        view_id: u64,
        stream_id: u64,
        prev_hash: [u8; 16],
        payload: &[u8],
        timestamp_ns: u64,
        flags: u16,
        schema_version: u16,
    ) -> Self {
        let payload_hash = compute_payload_hash(payload);
        let payload_size = payload.len() as u32;

        let mut header = LogHeader {
            header_checksum: 0, // Placeholder, computed below
            payload_size,
            index,
            view_id,
            stream_id,
            prev_hash,
            payload_hash,
            timestamp_ns,
            flags,
            schema_version,
            reserved: 0,
        };

        header.header_checksum = header.compute_checksum();
        header
    }

    /// Compute CRC32C of header bytes [4..79].
    /// Per log_format.md: Polynomial 0x1EDC6F41, Initial 0xFFFFFFFF, Final XOR 0xFFFFFFFF.
    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.as_bytes();
        // CRC32C of bytes [4..80] (excluding the checksum field itself)
        crc32c::crc32c(&bytes[4..])
    }

    /// Verify the header checksum.
    pub fn verify_checksum(&self) -> bool {
        self.header_checksum == self.compute_checksum()
    }

    /// Convert header to raw bytes.
    ///
    /// # Safety
    /// LogHeader is #[repr(C)] with no padding, so this is safe.
    pub fn as_bytes(&self) -> &[u8; HEADER_SIZE] {
        // SAFETY: LogHeader is #[repr(C)], exactly 80 bytes, no padding.
        // The struct layout matches the on-disk format exactly.
        unsafe { &*(self as *const LogHeader as *const [u8; HEADER_SIZE]) }
    }

    /// Create header from raw bytes.
    /// Reads fields in little-endian order per log_format.md.
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        let mut header = LogHeader {
            header_checksum: 0,
            payload_size: 0,
            index: 0,
            view_id: 0,
            stream_id: 0,
            prev_hash: [0u8; 16],
            payload_hash: [0u8; 16],
            timestamp_ns: 0,
            flags: 0,
            schema_version: 0,
            reserved: 0,
        };

        header.header_checksum = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        header.payload_size = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        header.index = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        header.view_id = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19],
            bytes[20], bytes[21], bytes[22], bytes[23],
        ]);
        header.stream_id = u64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27],
            bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        header.prev_hash.copy_from_slice(&bytes[32..48]);
        header.payload_hash.copy_from_slice(&bytes[48..64]);
        header.timestamp_ns = u64::from_le_bytes([
            bytes[64], bytes[65], bytes[66], bytes[67],
            bytes[68], bytes[69], bytes[70], bytes[71],
        ]);
        header.flags = u16::from_le_bytes([bytes[72], bytes[73]]);
        header.schema_version = u16::from_le_bytes([bytes[74], bytes[75]]);
        header.reserved = u32::from_le_bytes([bytes[76], bytes[77], bytes[78], bytes[79]]);

        header
    }

    /// Check if header is all zeros (end of log marker).
    #[allow(dead_code)]
    pub fn is_zero(&self) -> bool {
        self.as_bytes().iter().all(|&b| b == 0)
    }
}

/// Compute truncated BLAKE3 hash of payload (first 16 bytes).
/// Standardized to 16 bytes (128-bit) for uniform cryptographic strength.
pub fn compute_payload_hash(payload: &[u8]) -> [u8; 16] {
    let hash = blake3::hash(payload);
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&hash.as_bytes()[..16]);
    truncated
}

/// Compute the chain hash for an entry.
/// Per log_format.md: ChainHash(N) = BLAKE3(Header_N[4..71] || Payload_N)
/// Excludes header_checksum (bytes 0-4) and physical padding.
pub fn compute_chain_hash(header: &LogHeader, payload: &[u8]) -> [u8; 16] {
    let header_bytes = header.as_bytes();
    let mut hasher = blake3::Hasher::new();
    // Hash header bytes [4..72] (excluding checksum at [0..4])
    hasher.update(&header_bytes[4..]);
    // Hash payload
    hasher.update(payload);
    let hash = hasher.finalize();
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&hash.as_bytes()[..16]);
    truncated
}

/// Calculate padding needed to align next entry to 8-byte boundary.
/// Per log_format.md: pad_len = (8 - (payload_size % 8)) % 8
pub fn calculate_padding(payload_size: u32) -> usize {
    (8 - (payload_size as usize % 8)) % 8
}

// =============================================================================
// LOG FILE METADATA HEADER
// =============================================================================

/// Log file metadata header size (64 bytes, cache-line aligned).
///
/// This header is written at the start of the log file and contains
/// metadata needed for truncated logs.
///
/// | Offset | Field           | Size  | Description                              |
/// |--------|-----------------|-------|------------------------------------------|
/// | 0      | magic           | 4     | Magic bytes: "PLOG"                      |
/// | 4      | version         | 4     | Format version (currently 1)             |
/// | 8      | base_index      | 8     | First index present in this file         |
/// | 16     | base_prev_hash  | 16    | Chain hash of entry (base_index - 1)     |
/// | 32     | header_checksum | 4     | CRC32C of bytes [0..32] + [36..64]       |
/// | 36     | reserved        | 28    | Reserved for future use (zeros)          |
pub const LOG_METADATA_SIZE: usize = 64;

/// Log file magic bytes: "CLOG" (Chronon LOG)
pub const LOG_MAGIC: [u8; 4] = [0x50, 0x4C, 0x4F, 0x47];

/// Current log format version
pub const LOG_VERSION: u32 = 1;

/// Log file metadata header.
///
/// Written at offset 0 of every log file. For non-truncated logs,
/// base_index = 0 and base_prev_hash = GENESIS_HASH.
#[derive(Clone, Copy, Debug)]
pub struct LogMetadata {
    /// Magic bytes for file identification.
    pub magic: [u8; 4],
    /// Format version.
    pub version: u32,
    /// First index present in this file.
    /// For non-truncated logs, this is 0.
    /// For truncated logs, this is snapshot.last_included_index + 1.
    pub base_index: u64,
    /// Chain hash of entry at (base_index - 1).
    /// For non-truncated logs (base_index = 0), this is GENESIS_HASH.
    /// For truncated logs, this is snapshot.chain_hash.
    pub base_prev_hash: [u8; 16],
    /// CRC32C checksum of the metadata (excluding checksum field itself).
    pub checksum: u32,
}

impl LogMetadata {
    /// Create metadata for a new (non-truncated) log.
    pub fn new_empty() -> Self {
        let mut meta = LogMetadata {
            magic: LOG_MAGIC,
            version: LOG_VERSION,
            base_index: 0,
            base_prev_hash: GENESIS_HASH,
            checksum: 0,
        };
        meta.checksum = meta.compute_checksum();
        meta
    }

    /// Create metadata for a truncated log.
    pub fn new_truncated(base_index: u64, base_prev_hash: [u8; 16]) -> Self {
        let mut meta = LogMetadata {
            magic: LOG_MAGIC,
            version: LOG_VERSION,
            base_index,
            base_prev_hash,
            checksum: 0,
        };
        meta.checksum = meta.compute_checksum();
        meta
    }

    /// Compute CRC32C checksum of metadata (excluding checksum field).
    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.as_bytes();
        // Checksum covers bytes [0..32] and [36..64], skipping checksum at [32..36]
        let mut hasher = crc32c::crc32c(&bytes[0..32]);
        hasher = crc32c::crc32c_append(hasher, &bytes[36..64]);
        hasher
    }

    /// Verify the metadata checksum.
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    /// Verify magic bytes.
    pub fn verify_magic(&self) -> bool {
        self.magic == LOG_MAGIC
    }

    /// Convert metadata to raw bytes.
    pub fn as_bytes(&self) -> [u8; LOG_METADATA_SIZE] {
        let mut bytes = [0u8; LOG_METADATA_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.base_index.to_le_bytes());
        bytes[16..32].copy_from_slice(&self.base_prev_hash);
        bytes[32..36].copy_from_slice(&self.checksum.to_le_bytes());
        // bytes[36..64] are reserved (zeros)
        bytes
    }

    /// Create metadata from raw bytes.
    pub fn from_bytes(bytes: &[u8; LOG_METADATA_SIZE]) -> Self {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);

        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let base_index = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        let mut base_prev_hash = [0u8; 16];
        base_prev_hash.copy_from_slice(&bytes[16..32]);

        let checksum = u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);

        LogMetadata {
            magic,
            version,
            base_index,
            base_prev_hash,
            checksum,
        }
    }
}

/// Calculate total frame size (header + payload + padding).
pub fn frame_size(payload_size: u32) -> usize {
    HEADER_SIZE + payload_size as usize + calculate_padding(payload_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(mem::size_of::<LogHeader>(), 80);
    }

    #[test]
    fn test_padding_calculation() {
        assert_eq!(calculate_padding(0), 0);
        assert_eq!(calculate_padding(1), 7);
        assert_eq!(calculate_padding(7), 1);
        assert_eq!(calculate_padding(8), 0);
        assert_eq!(calculate_padding(9), 7);
    }

    #[test]
    fn test_checksum_roundtrip() {
        let payload = b"test payload";
        let timestamp_ns = 1_000_000_000u64; // 1 second since epoch
        let header = LogHeader::new(
            0,
            1,
            0,
            GENESIS_HASH,
            payload,
            timestamp_ns,
            0,
            1,
        );
        assert!(header.verify_checksum());
        assert_eq!(header.timestamp_ns, timestamp_ns);
    }
    
    #[test]
    fn test_sentinel_v2_size() {
        // ENFORCES F7: Sentinel must be 16 bytes for full u64 index
        assert_eq!(SENTINEL_SIZE, 16);
    }
    
    #[test]
    fn test_sentinel_v2_roundtrip() {
        // ENFORCES F7: Sentinel stores full u64 index without truncation
        let sentinel = create_sentinel(12345);
        assert!(verify_sentinel(&sentinel, 12345));
        assert!(!verify_sentinel(&sentinel, 12346));
    }
    
    #[test]
    fn test_sentinel_v2_large_index() {
        // ENFORCES F7: Sentinel handles indices > u32::MAX
        let large_index: u64 = (u32::MAX as u64) + 1000;
        let sentinel = create_sentinel(large_index);
        assert!(verify_sentinel(&sentinel, large_index));
        
        // Verify it doesn't match truncated value
        let truncated = large_index as u32 as u64;
        assert!(!verify_sentinel(&sentinel, truncated));
    }
    
    #[test]
    fn test_sentinel_v2_max_index() {
        // ENFORCES F7: Sentinel handles u64::MAX - 1 (max valid index)
        let max_index = u64::MAX - 1;
        let sentinel = create_sentinel(max_index);
        assert!(verify_sentinel(&sentinel, max_index));
    }
    
    #[test]
    fn test_sentinel_magic_detection() {
        // Test is_sentinel_magic for v1 and v2
        assert!(is_sentinel_magic(&SENTINEL_MAGIC));
        assert!(is_sentinel_magic(&SENTINEL_MAGIC_V1));
        assert!(!is_sentinel_magic(&[0x00, 0x00, 0x00, 0x00]));
        assert!(!is_sentinel_magic(&[0xFF, 0xFF, 0xFF, 0xFF]));
    }
}
