//! Benchmark full-array reads across four store configurations, at two chunk sizes:
//! `FilesystemStore`, `ZipStorageAdapter<FilesystemStore>`, `MemoryStore`, and
//! `ZipStorageAdapter<MemoryStore>`.
#![allow(missing_docs)]

use std::{
    fs::File,
    io::{Cursor, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

use zarrs::{
    array::{Array, ArrayBuilder, DataType},
    array_subset::ArraySubset,
    storage::ReadableWritableListableStorage,
};
use zarrs_filesystem::FilesystemStore;
use zarrs_storage::{StoreKey, WritableStorageTraits, store::MemoryStore};
use zarrs_zip::ZipStorageAdapter;

const ARRAY_SHAPE: [u64; 3] = [512, 512, 512];
const CHUNK_SHAPE_SMALL: [u64; 3] = [32, 32, 32];
const CHUNK_SHAPE_LARGE: [u64; 3] = [256, 256, 256];
const TOTAL_BYTES: u64 = ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2];

/// Recursively zip the contents of a directory, matching the helper used in
/// `examples/zip_array_write_read.rs` and `tests/test_zip_storage.rs`, generalised
/// to any `Write + Seek` writer and returning the finished writer so it can be used
/// for both on-disk and in-memory zips.
fn zip_dir<I: Iterator<Item = walkdir::DirEntry>, W: Write + Seek>(
    it: I,
    prefix: &str,
    writer: W,
    method: zip::CompressionMethod,
) -> zip::result::ZipResult<W> {
    let mut zip = zip::ZipWriter::new(writer);
    let options = SimpleFileOptions::default().compression_method(method);
    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(Path::new(prefix)).unwrap();
        if path.is_file() {
            #[allow(deprecated)]
            zip.start_file_from_path(name, options)?;
            let mut f = File::open(path)?;
            f.read_to_end(&mut buffer)?;
            zip.write_all(&buffer)?;
            buffer.clear();
        } else if !name.as_os_str().is_empty() {
            #[allow(deprecated)]
            zip.add_directory_from_path(name, options)?;
        }
    }
    zip.finish()
}

/// Build and populate a `[512, 512, 512]` `u8` array with the given chunk shape in a
/// fresh `FilesystemStore` rooted at a new temporary directory.
fn populate_filesystem_array(chunk_shape: [u64; 3]) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let storage: ReadableWritableListableStorage = Arc::new(FilesystemStore::new(&path).unwrap());

    let array = ArrayBuilder::new(
        ARRAY_SHAPE.to_vec(),
        chunk_shape.to_vec(),
        DataType::UInt8,
        0u8,
    )
    .build(storage, "/")
    .unwrap();
    array.store_metadata().unwrap();

    let data = vec![1u8; usize::try_from(TOTAL_BYTES).unwrap()];
    array.store_array_subset(&array.subset_all(), data).unwrap();

    (temp_dir, path)
}

/// Zip a populated directory to a file on disk, returning a `TempDir` guard for the
/// zip's parent directory and the path to the zip file.
fn zip_to_disk(src_dir: &Path) -> (TempDir, PathBuf) {
    let zip_temp_dir = TempDir::new().unwrap();
    let zip_path = zip_temp_dir.path().join("array.zip");
    let file = File::create(&zip_path).unwrap();
    zip_dir(
        WalkDir::new(src_dir).into_iter().filter_map(Result::ok),
        src_dir.to_str().unwrap(),
        file,
        zip::CompressionMethod::Stored,
    )
    .unwrap();
    (zip_temp_dir, zip_path)
}

/// Zip a populated directory into an in-memory buffer.
fn zip_to_memory(src_dir: &Path) -> Vec<u8> {
    let cursor = zip_dir(
        WalkDir::new(src_dir).into_iter().filter_map(Result::ok),
        src_dir.to_str().unwrap(),
        Cursor::new(Vec::new()),
        zip::CompressionMethod::Stored,
    )
    .unwrap();
    cursor.into_inner()
}

/// Guards that must be kept alive for the duration of a benchmark (temp directories
/// backing a `FilesystemStore` or an on-disk zip file).
struct Fixture<TStorage: ?Sized> {
    array: Array<TStorage>,
    _guards: Vec<TempDir>,
}

fn open_filesystem(chunk_shape: [u64; 3]) -> Fixture<FilesystemStore> {
    let (temp_dir, path) = populate_filesystem_array(chunk_shape);
    let storage = Arc::new(FilesystemStore::new(&path).unwrap());
    let array = Array::open(storage, "/").unwrap();
    Fixture {
        array,
        _guards: vec![temp_dir],
    }
}

fn open_zip_filesystem(chunk_shape: [u64; 3]) -> Fixture<ZipStorageAdapter<FilesystemStore>> {
    let (data_dir, path) = populate_filesystem_array(chunk_shape);
    let (zip_dir, zip_path) = zip_to_disk(&path);
    let fs_store = Arc::new(FilesystemStore::new(&zip_path).unwrap());
    let zip_store = Arc::new(ZipStorageAdapter::new(fs_store, StoreKey::root()).unwrap());
    let array = Array::open(zip_store, "/").unwrap();
    Fixture {
        array,
        _guards: vec![data_dir, zip_dir],
    }
}

fn open_memory(chunk_shape: [u64; 3]) -> Fixture<MemoryStore> {
    let storage = Arc::new(MemoryStore::default());
    let array = ArrayBuilder::new(
        ARRAY_SHAPE.to_vec(),
        chunk_shape.to_vec(),
        DataType::UInt8,
        0u8,
    )
    .build(storage, "/")
    .unwrap();
    array.store_metadata().unwrap();

    let data = vec![1u8; usize::try_from(TOTAL_BYTES).unwrap()];
    array.store_array_subset(&array.subset_all(), data).unwrap();

    Fixture {
        array,
        _guards: Vec::new(),
    }
}

fn open_zip_memory(chunk_shape: [u64; 3]) -> Fixture<ZipStorageAdapter<MemoryStore>> {
    let (data_dir, path) = populate_filesystem_array(chunk_shape);
    let zip_bytes = zip_to_memory(&path);

    let memory_store = Arc::new(MemoryStore::default());
    memory_store
        .set(&StoreKey::root(), zip_bytes.into())
        .unwrap();
    let zip_store = Arc::new(ZipStorageAdapter::new(memory_store, StoreKey::root()).unwrap());
    let array = Array::open(zip_store, "/").unwrap();
    Fixture {
        array,
        _guards: vec![data_dir],
    }
}

fn bench_chunk_size(c: &mut Criterion, group_name: &str, chunk_shape: [u64; 3]) {
    let filesystem = open_filesystem(chunk_shape);
    let zip_filesystem = open_zip_filesystem(chunk_shape);
    let memory = open_memory(chunk_shape);
    let zip_memory = open_zip_memory(chunk_shape);

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Bytes(TOTAL_BYTES));

    let subset = ArraySubset::new_with_shape(ARRAY_SHAPE.to_vec());

    group.bench_function("filesystem", |b| {
        b.iter(|| {
            let _: zarrs::array::ArrayBytes =
                filesystem.array.retrieve_array_subset(&subset).unwrap();
        });
    });
    group.bench_function("zip_filesystem", |b| {
        b.iter(|| {
            let _: zarrs::array::ArrayBytes =
                zip_filesystem.array.retrieve_array_subset(&subset).unwrap();
        });
    });
    group.bench_function("memory", |b| {
        b.iter(|| {
            let _: zarrs::array::ArrayBytes = memory.array.retrieve_array_subset(&subset).unwrap();
        });
    });
    group.bench_function("zip_memory", |b| {
        b.iter(|| {
            let _: zarrs::array::ArrayBytes =
                zip_memory.array.retrieve_array_subset(&subset).unwrap();
        });
    });

    group.finish();
}

fn bench_read_small_chunks(c: &mut Criterion) {
    bench_chunk_size(c, "read_small_chunks", CHUNK_SHAPE_SMALL);
}

fn bench_read_large_chunks(c: &mut Criterion) {
    bench_chunk_size(c, "read_large_chunks", CHUNK_SHAPE_LARGE);
}

criterion_group!(benches, bench_read_small_chunks, bench_read_large_chunks);
criterion_main!(benches);
