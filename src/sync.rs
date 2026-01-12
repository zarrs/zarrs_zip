use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::ZipEntry;

use super::{ZipStorageAdapter, ZipStorageAdapterCreateError};
use rc_zip::{
    Entry, EntryKind,
    fsm::{ArchiveFsm, EntryFsm, FsmResult},
    parse::Method,
};
use zarrs_storage::{
    Bytes, ListableStorageTraits, MaybeBytesIterator, ReadableStorageTraits, StorageError,
    StoreKey, StorePrefix, StorePrefixes,
    byte_range::{ByteRange, ByteRangeIterator, InvalidByteRangeError},
};
use zarrs_storage::{StoreKeys, StoreKeysPrefixes};

impl<TStorage: ?Sized + ReadableStorageTraits> ZipStorageAdapter<TStorage> {
    /// Create a new zip storage adapter.
    ///
    /// # Errors
    /// Returns a [`ZipStorageAdapterCreateError`] if the store value at `key` is not a valid zip file.
    pub fn new(
        storage: Arc<TStorage>,
        key: StoreKey,
    ) -> Result<Self, ZipStorageAdapterCreateError> {
        Self::new_with_path(storage, key, "")
    }

    /// Create a new zip storage adapter to `path` within the zip file.
    ///
    /// # Errors
    /// Returns a [`ZipStorageAdapterCreateError`] if the store value at `key` is not a valid zip file.
    pub fn new_with_path<T: Into<PathBuf>>(
        storage: Arc<TStorage>,
        key: StoreKey,
        path: T,
    ) -> Result<Self, ZipStorageAdapterCreateError> {
        let zip_path = path.into();

        // Get zip file size
        let size = storage
            .size_key(&key)?
            .ok_or_else(|| StorageError::UnknownKeySize(key.clone()))?;

        // Parse the archive using ArchiveFsm
        let archive = Self::parse_archive(&storage, &key, size)?;

        // Build entries map and sorted entries list
        let mut entries: HashMap<StoreKey, Entry> = HashMap::new();
        let mut sorted_entries: Vec<ZipEntry> = Vec::new();
        for entry in archive.entries() {
            if let Some(stripped) = Self::strip_zip_path_prefix(&entry.name, &zip_path) {
                match entry.kind() {
                    EntryKind::File => {
                        let store_key = StoreKey::try_from(stripped)?;
                        entries.insert(store_key.clone(), entry.clone()); // FIXME: It'd be nice to avoid the clone, needs rc-zip change
                        sorted_entries.push(ZipEntry::Key(store_key));
                    }
                    EntryKind::Directory => {
                        let store_prefix = StorePrefix::try_from(stripped)?;
                        sorted_entries.push(ZipEntry::Prefix(store_prefix));
                    }
                    EntryKind::Symlink => {
                        // Ignore symlinks
                    }
                }
            }
        }
        sorted_entries.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        Ok(Self {
            size,
            storage,
            key,
            entries,
            sorted_entries,
        })
    }

    /// Parse the zip archive using `ArchiveFsm`.
    fn parse_archive(
        storage: &Arc<TStorage>,
        key: &StoreKey,
        size: u64,
    ) -> Result<rc_zip::parse::Archive, ZipStorageAdapterCreateError> {
        let mut fsm = ArchiveFsm::new(size);

        loop {
            // Check if FSM needs more data
            if let Some(offset) = fsm.wants_read() {
                let space = fsm.space();
                // Don't request more than what's left in the file
                let remaining = size.saturating_sub(offset);
                let to_read = (space.len() as u64).min(remaining);

                if to_read > 0 {
                    // Read from storage at the requested offset
                    let byte_range = ByteRange::FromStart(offset, Some(to_read));
                    let data = storage.get_partial(key, byte_range)?.ok_or_else(|| {
                        ZipStorageAdapterCreateError::ZipError("Cannot read zip data".to_string())
                    })?;

                    // Copy data into FSM buffer
                    let copy_len = data.len().min(space.len());
                    space[..copy_len].copy_from_slice(&data[..copy_len]);
                    fsm.fill(copy_len);
                } else {
                    // No more data to read, signal EOF by filling 0 bytes
                    fsm.fill(0);
                }
            }

            // Process the data
            match fsm.process() {
                Ok(FsmResult::Continue(next_fsm)) => {
                    fsm = next_fsm;
                }
                Ok(FsmResult::Done(archive)) => {
                    return Ok(archive);
                }
                Err(e) => {
                    return Err(ZipStorageAdapterCreateError::ZipError(e.to_string()));
                }
            }
        }
    }

    fn get_impl(
        &self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'_>,
    ) -> Result<MaybeBytesIterator<'_>, StorageError> {
        let Some(entry) = self.get_entry(key) else {
            return Ok(None);
        };

        let byte_ranges: Vec<ByteRange> = byte_ranges.collect();

        // Validate that all byte ranges are within bounds
        for range in &byte_ranges {
            let end = match range {
                ByteRange::FromStart(start, Some(len)) => start.saturating_add(*len),
                ByteRange::FromStart(start, None) => *start, // Reading to end is always valid if start is valid
                ByteRange::Suffix(_) => 0,                   // Suffix is clamped, always valid
            };
            if end > entry.uncompressed_size {
                return Err(InvalidByteRangeError::new(*range, entry.uncompressed_size).into());
            }
        }

        match entry.method {
            Method::Store => {
                // Fast path: read directly from storage
                self.get_stored_entry(entry, &byte_ranges)
            }
            _ => {
                // Decompress the entry using EntryFsm
                self.get_compressed_entry(entry, &byte_ranges)
            }
        }
    }

    /// Fast path for stored (uncompressed) entries.
    fn get_stored_entry(
        &self,
        entry: &Entry,
        byte_ranges: &[ByteRange],
    ) -> Result<MaybeBytesIterator<'_>, StorageError> {
        // Calculate data offset by reading local file header
        let data_offset = self
            .calculate_data_offset(entry.header_offset)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        // Translate relative byte ranges to absolute zip file offsets
        let translated: Vec<ByteRange> = byte_ranges
            .iter()
            .map(|range| match range {
                ByteRange::FromStart(start, len) => {
                    let actual_len = len.unwrap_or(entry.uncompressed_size.saturating_sub(*start));
                    ByteRange::FromStart(data_offset + start, Some(actual_len))
                }
                ByteRange::Suffix(len) => {
                    let start = data_offset + entry.uncompressed_size.saturating_sub(*len);
                    ByteRange::FromStart(start, Some((*len).min(entry.uncompressed_size)))
                }
            })
            .collect();

        // Retrieve the bytes
        self.storage
            .get_partial_many(&self.key, Box::new(translated.into_iter()))?
            .ok_or_else(|| StorageError::Other("Entry data not found".to_string()))
            .map(Some)
    }

    /// Slower path for compressed entries using `EntryFsm`.
    ///
    /// Decodes the entire entry and then slices out the requested byte ranges.
    #[allow(clippy::cast_possible_truncation)]
    fn get_compressed_entry(
        &self,
        entry: &Entry,
        byte_ranges: &[ByteRange],
    ) -> Result<MaybeBytesIterator<'_>, StorageError> {
        let decompressed = self.decompress_entry(entry)?;

        let mut results = Vec::with_capacity(byte_ranges.len());
        for range in byte_ranges {
            let range = range.to_range_usize(entry.uncompressed_size);
            results.push(Ok(Bytes::copy_from_slice(&decompressed[range])));
        }

        Ok(Some(Box::new(results.into_iter())))
    }

    /// Decompress an entry using `EntryFsm`.
    #[allow(clippy::cast_possible_truncation)]
    fn decompress_entry(&self, entry: &Entry) -> Result<Vec<u8>, StorageError> {
        // Create EntryFsm with the entry
        let mut fsm = EntryFsm::new(Some(entry.clone()), None);

        // Read position starts at header_offset (EntryFsm will parse local header first)
        let mut read_offset = entry.header_offset;

        // Pre-allocate output buffer
        let expected_size = entry.uncompressed_size as usize;
        let mut decompressed: Vec<u8> = Vec::with_capacity(expected_size);
        let mut write_offset = 0usize;

        loop {
            // Feed data to FSM if it wants to read
            if fsm.wants_read() {
                let space = fsm.space();
                // Don't request more than what's left in the file
                let remaining = self.size.saturating_sub(read_offset);
                let to_read = (space.len() as u64).min(remaining);

                if to_read > 0 {
                    let byte_range = ByteRange::FromStart(read_offset, Some(to_read));

                    let data = self
                        .storage
                        .get_partial(&self.key, byte_range)?
                        .ok_or_else(|| {
                            StorageError::Other("Cannot read compressed data".to_string())
                        })?;

                    let copy_len = data.len().min(space.len());
                    space[..copy_len].copy_from_slice(&data[..copy_len]);
                    let filled = fsm.fill(copy_len);
                    read_offset += filled as u64;
                } else {
                    // No more data to read, signal EOF
                    fsm.fill(0);
                }
            }

            // Write directly into the spare capacity
            // SAFETY: We pass uninitialized memory to fsm.process, which will write
            // `outcome.bytes_written` bytes, and won't read.
            let spare = decompressed.spare_capacity_mut();
            let out_slice = unsafe {
                std::slice::from_raw_parts_mut(
                    spare.as_mut_ptr().cast::<u8>(),
                    expected_size.saturating_sub(write_offset),
                )
            };

            match fsm.process(out_slice) {
                Ok(FsmResult::Continue((next_fsm, outcome))) => {
                    write_offset += outcome.bytes_written;
                    fsm = next_fsm;
                }
                Ok(FsmResult::Done(_buffer)) => {
                    // Decompression complete
                    break;
                }
                Err(e) => {
                    return Err(StorageError::Other(format!("Decompression error: {e}")));
                }
            }
        }

        // Verify decompressed size matches expected
        if write_offset != expected_size {
            return Err(StorageError::Other(format!(
                "zip decompressed entry size mismatch: expected {expected_size}, got {write_offset}"
            )));
        }

        // SAFETY: We verified that write_offset == expected_size, and fsm.process
        // has initialized all bytes up to write_offset.
        unsafe {
            decompressed.set_len(expected_size);
        }

        Ok(decompressed)
    }

    /// Calculate the data offset by reading the local file header.
    ///
    /// The local file header is 30 bytes fixed + variable name/extra fields.
    fn calculate_data_offset(
        &self,
        header_offset: u64,
    ) -> Result<u64, ZipStorageAdapterCreateError> {
        // Read 30-byte local file header
        let byte_range = ByteRange::FromStart(header_offset, Some(30));
        let header = self
            .storage
            .get_partial(&self.key, byte_range)?
            .ok_or_else(|| {
                ZipStorageAdapterCreateError::ZipError("Cannot read local file header".to_string())
            })?;

        if header.len() < 30 {
            return Err(ZipStorageAdapterCreateError::ZipError(
                "Local file header too short".to_string(),
            ));
        }

        // Local file header structure:
        // Offset 26: filename length (2 bytes, little-endian)
        // Offset 28: extra field length (2 bytes, little-endian)
        let filename_len = u64::from(u16::from_le_bytes([header[26], header[27]]));
        let extra_len = u64::from(u16::from_le_bytes([header[28], header[29]]));

        Ok(header_offset + 30 + filename_len + extra_len)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for ZipStorageAdapter<TStorage>
{
    fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<MaybeBytesIterator<'a>, StorageError> {
        self.get_impl(key, byte_ranges)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(self.get_entry(key).map(|e| e.uncompressed_size))
    }

    fn supports_get_partial(&self) -> bool {
        true
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ListableStorageTraits
    for ZipStorageAdapter<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        // Filter to only keys, already sorted
        Ok(self
            .sorted_entries
            .iter()
            .filter_map(|e| match e {
                ZipEntry::Key(k) => Some(k.clone()),
                ZipEntry::Prefix(_) => None,
            })
            .collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        // Use binary search to find matching range, filter to keys only
        Ok(self
            .entries_with_prefix(prefix)
            .iter()
            .filter_map(|e| match e {
                ZipEntry::Key(k) => Some(k.clone()),
                ZipEntry::Prefix(_) => None,
            })
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let mut keys: StoreKeys = vec![];
        let mut prefixes: StorePrefixes = vec![];

        // Use binary search to find matching range
        for entry in self.entries_with_prefix(prefix) {
            match entry {
                ZipEntry::Key(key) => {
                    let parent = key.parent();
                    if &parent == prefix {
                        keys.push(key.clone());
                    } else if let Some(child_prefix) = Self::immediate_child_prefix(key, prefix) {
                        if prefixes.last() != Some(&child_prefix) {
                            prefixes.push(child_prefix);
                        }
                    }
                }
                ZipEntry::Prefix(p) => {
                    // Check if this prefix is an immediate child of the search prefix
                    let p_str = p.as_str();
                    let prefix_str = prefix.as_str();
                    if let Some(suffix) = p_str.strip_prefix(prefix_str) {
                        // Skip if suffix is empty (the prefix itself)
                        if suffix.is_empty() {
                            continue;
                        }
                        // Check if it's an immediate child (no additional '/' before the trailing one)
                        let trimmed = suffix.trim_end_matches('/');
                        if !trimmed.contains('/') && prefixes.last() != Some(p) {
                            prefixes.push(p.clone());
                        }
                    }
                }
            }
        }

        // Keys and prefixes are already sorted since sorted_entries is sorted
        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    fn size(&self) -> Result<u64, StorageError> {
        Ok(self.size)
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        // Use binary search to find matching range, then lookup in HashMap for keys only
        Ok(self
            .entries_with_prefix(prefix)
            .iter()
            .filter_map(|e| match e {
                ZipEntry::Key(k) => self.entries.get(k),
                ZipEntry::Prefix(_) => None,
            })
            .map(|e| e.compressed_size)
            .sum())
    }
}
