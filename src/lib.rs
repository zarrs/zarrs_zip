//! A storage adapter for `zip` files for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! ```
//! # use std::path::PathBuf;
//! # use std::sync::Arc;
//! use zarrs_storage::StoreKey;
//! use zarrs_filesystem::FilesystemStore;
//! use zarrs_zip::ZipStorageAdapter;
//!
//! let fs_root = PathBuf::from("/path/to/a/directory");
//! # let fs_root = PathBuf::from("tests/");
//! let fs_store = Arc::new(FilesystemStore::new(&fs_root)?);
//! let zip_key = StoreKey::new("zarr.zip")?;
//! let zip_store = Arc::new(ZipStorageAdapter::new(fs_store, zip_key)?);
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! See a full example at [examples/zip_array_write_read.rs](https://github.com/zarrs/zarrs_zip/blob/main/examples/zip_array_write_read.rs).
//!
//! ## `zarrs` Version Compatibility Matrix
//!
#![doc = include_str!("../doc/version_compatibility_matrix.md")]
//!
//! ## Licence
//! `zarrs_zip` is licensed under either of
//! - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs_zip/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//! - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs_zip/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod sync;

#[cfg(feature = "async")]
mod r#async;

use zarrs_storage::{StorageError, StoreKey, StoreKeyError, StorePrefix, StorePrefixError};

use rc_zip::parse::Entry;
use thiserror::Error;

use std::collections::HashMap;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// An entry in the zip archive (either a file or directory).
#[derive(Debug, Clone, PartialEq, Eq)]
enum ZipEntry {
    Key(StoreKey),
    Prefix(StorePrefix),
}

impl ZipEntry {
    fn as_str(&self) -> &str {
        match self {
            ZipEntry::Key(k) => k.as_str(),
            ZipEntry::Prefix(p) => p.as_str(),
        }
    }
}

/// A zip storage adapter.
pub struct ZipStorageAdapter<TStorage: ?Sized> {
    /// Total size of the zip file.
    size: u64,
    /// Reference to underlying storage.
    storage: Arc<TStorage>,
    /// Store key for the zip file.
    key: StoreKey,
    /// `HashMap` for O(1) entry lookup by key.
    entries: HashMap<StoreKey, Entry>,
    /// Sorted entries (keys and prefixes) for listing operations.
    sorted_entries: Vec<ZipEntry>,
}

impl<TStorage: ?Sized> ZipStorageAdapter<TStorage> {
    fn strip_zip_path_prefix<'a>(name: &'a str, zip_path: &Path) -> Option<&'a str> {
        let prefix = zip_path.to_str().unwrap_or("");
        name.strip_prefix(prefix).filter(|&n| !n.is_empty())
    }

    /// Get an entry by key using O(1) `HashMap` lookup.
    fn get_entry(&self, key: &StoreKey) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// Find the range of entries matching a prefix using binary search.
    fn entries_with_prefix(&self, prefix: &StorePrefix) -> &[ZipEntry] {
        let prefix_str = prefix.as_str();

        // Find start index: first entry >= prefix
        let start = self
            .sorted_entries
            .partition_point(|e| e.as_str() < prefix_str);

        // Find end index: first entry that doesn't start with prefix
        let end = self.sorted_entries[start..]
            .partition_point(|e| e.as_str().starts_with(prefix_str))
            + start;

        &self.sorted_entries[start..end]
    }

    /// Get the immediate child prefix of a key relative to a parent prefix.
    fn immediate_child_prefix(key: &StoreKey, prefix: &StorePrefix) -> Option<StorePrefix> {
        let key_str = key.as_str();
        let prefix_str = prefix.as_str();

        // Get the part after the prefix
        let suffix = key_str.strip_prefix(prefix_str)?;

        // Find the first '/' in the suffix to get the immediate child directory
        if let Some(slash_pos) = suffix.find('/') {
            let child = &suffix[..=slash_pos];
            let full_prefix = format!("{prefix_str}{child}");
            StorePrefix::try_from(full_prefix.as_str()).ok()
        } else {
            None
        }
    }
}

/// A zip store creation error.
#[derive(Debug, Error)]
pub enum ZipStorageAdapterCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// An existing directory.
    #[error("{0} is an existing directory, not a zip file")]
    ExistingDir(PathBuf),
    /// A zip error.
    #[error("{0}")]
    ZipError(String),
    /// A storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// Invalid store key.
    #[error(transparent)]
    InvalidStoreKey(#[from] StoreKeyError),
    /// Invalid store prefix.
    #[error(transparent)]
    InvalidStorePrefix(#[from] StorePrefixError),
}
