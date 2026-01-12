#![allow(missing_docs)]

use std::{
    error::Error,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

use zarrs_filesystem::FilesystemStore;
use zarrs_storage::{
    ListableStorageTraits, ReadableStorageTraits, StoreKey, WritableStorageTraits,
    store::MemoryStore,
};
use zarrs_zip::ZipStorageAdapter;

// https://github.com/zip-rs/zip/blob/master/examples/write_dir.rs
fn zip_dir<I: Iterator<Item = walkdir::DirEntry>>(
    it: I,
    prefix: &str,
    writer: File,
    method: zip::CompressionMethod,
) -> zip::result::ZipResult<()> {
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
    zip.finish()?;
    Result::Ok(())
}

fn zip_write(path: &Path) -> Result<(), Box<dyn Error>> {
    let tmp_path = tempfile::TempDir::new()?;
    let tmp_path = tmp_path.path();
    let store = FilesystemStore::new(tmp_path)?.sorted();
    store.set(&"a/b/zarr.json".try_into()?, vec![0, 1, 2, 3].into())?;
    store.set(&"a/c/zarr.json".try_into()?, vec![].into())?;
    store.set(&"a/d/e/zarr.json".try_into()?, vec![].into())?;
    store.set(&"a/f/g/zarr.json".try_into()?, vec![].into())?;
    store.set(&"a/f/h/zarr.json".try_into()?, vec![].into())?;
    store.set(&"b/zarr.json".try_into()?, vec![].into())?;
    store.set(&"b/c/d/zarr.json".try_into()?, vec![].into())?;
    store.set(&"c/zarr.json".try_into()?, vec![].into())?;

    let walkdir = WalkDir::new(tmp_path);

    let file = File::create(path).unwrap();
    zip_dir(
        &mut walkdir.into_iter().filter_map(std::result::Result::ok),
        tmp_path.to_str().unwrap(),
        file,
        zip::CompressionMethod::Stored,
    )?;

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
fn zip_root() -> Result<(), Box<dyn Error>> {
    let path = tempfile::TempDir::new()?;
    let mut path = path.path().to_path_buf();
    let store = FilesystemStore::new(path.clone())?;

    path.push("test.zip");
    zip_write(&path)?;

    let store = Arc::new(ZipStorageAdapter::new(
        store.into(),
        StoreKey::new("test.zip")?,
    )?);

    assert_eq!(
        store.list()?,
        &[
            "a/b/zarr.json".try_into()?,
            "a/c/zarr.json".try_into()?,
            "a/d/e/zarr.json".try_into()?,
            "a/f/g/zarr.json".try_into()?,
            "a/f/h/zarr.json".try_into()?,
            "b/c/d/zarr.json".try_into()?,
            "b/zarr.json".try_into()?,
            "c/zarr.json".try_into()?,
        ]
    );
    assert_eq!(
        store.list_prefix(&"a/".try_into()?)?,
        &[
            "a/b/zarr.json".try_into()?,
            "a/c/zarr.json".try_into()?,
            "a/d/e/zarr.json".try_into()?,
            "a/f/g/zarr.json".try_into()?,
            "a/f/h/zarr.json".try_into()?,
        ]
    );
    assert_eq!(
        store.list_prefix(&"a/d/".try_into()?)?,
        &["a/d/e/zarr.json".try_into()?]
    );
    assert_eq!(
        store.list_prefix(&"".try_into()?)?,
        &[
            "a/b/zarr.json".try_into()?,
            "a/c/zarr.json".try_into()?,
            "a/d/e/zarr.json".try_into()?,
            "a/f/g/zarr.json".try_into()?,
            "a/f/h/zarr.json".try_into()?,
            "b/c/d/zarr.json".try_into()?,
            "b/zarr.json".try_into()?,
            "c/zarr.json".try_into()?,
        ]
    );

    let list = store.list_dir(&"a/".try_into()?)?;
    assert_eq!(list.keys(), &[]);
    assert_eq!(
        list.prefixes(),
        &[
            "a/b/".try_into()?,
            "a/c/".try_into()?,
            "a/d/".try_into()?,
            "a/f/".try_into()?,
        ]
    );

    assert_eq!(
        store.get(&"a/b/zarr.json".try_into()?)?.unwrap(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        store.get(&"a/c/zarr.json".try_into()?)?.unwrap(),
        Vec::<u8>::new().as_slice()
    );

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
fn zip_path() -> Result<(), Box<dyn Error>> {
    let path = tempfile::TempDir::new()?;
    let mut path = path.path().to_path_buf();
    let store = FilesystemStore::new(path.clone())?;
    path.push("test.zip");
    zip_write(&path)?;

    let store = Arc::new(ZipStorageAdapter::new_with_path(
        store.into(),
        StoreKey::new("test.zip")?,
        "a/",
    )?);

    assert_eq!(
        store.list()?,
        &[
            "b/zarr.json".try_into()?,
            "c/zarr.json".try_into()?,
            "d/e/zarr.json".try_into()?,
            "f/g/zarr.json".try_into()?,
            "f/h/zarr.json".try_into()?,
        ]
    );
    assert_eq!(store.list_prefix(&"a/".try_into()?)?, &[]);
    assert_eq!(
        store.list_prefix(&"d/".try_into()?)?,
        &["d/e/zarr.json".try_into()?]
    );
    assert_eq!(
        store.list_prefix(&"".try_into()?)?,
        &[
            "b/zarr.json".try_into()?,
            "c/zarr.json".try_into()?,
            "d/e/zarr.json".try_into()?,
            "f/g/zarr.json".try_into()?,
            "f/h/zarr.json".try_into()?,
        ]
    );

    let list = store.list_dir(&"".try_into()?)?;
    assert_eq!(list.keys(), &[]);
    assert_eq!(
        list.prefixes(),
        &[
            "b/".try_into()?,
            "c/".try_into()?,
            "d/".try_into()?,
            "f/".try_into()?,
        ]
    );

    assert_eq!(
        store.get(&"b/zarr.json".try_into()?)?.unwrap(),
        vec![0, 1, 2, 3]
    );
    // assert_eq!(store.get(&"c/zarr.json".try_into()?)?, Vec::<u8>::new().as_slice());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
fn zip_empty_directory() -> Result<(), Box<dyn Error>> {
    let tmp_dir = tempfile::TempDir::new()?;
    let zip_path = tmp_dir.path().join("test.zip");

    // Create a zip file with an empty directory
    {
        let file = File::create(&zip_path)?;
        let mut zip = zip::ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        // Add a file
        zip.start_file("a/file.txt", options)?;
        zip.write_all(b"hello")?;

        // Add an empty directory
        zip.add_directory("a/empty/", options)?;

        // Add another file to verify the empty dir doesn't interfere
        zip.start_file("b/file.txt", options)?;
        zip.write_all(b"world")?;

        zip.finish()?;
    }

    let store = FilesystemStore::new(tmp_dir.path())?;
    let store = Arc::new(ZipStorageAdapter::new(
        store.into(),
        StoreKey::new("test.zip")?,
    )?);

    // list() should only return files
    assert_eq!(
        store.list()?,
        &["a/file.txt".try_into()?, "b/file.txt".try_into()?]
    );

    // list_dir at root should show a/ and b/
    let list = store.list_dir(&"".try_into()?)?;
    assert_eq!(list.keys(), &[]);
    assert_eq!(list.prefixes(), &["a/".try_into()?, "b/".try_into()?]);

    // list_dir at a/ should show the file and the empty directory
    let list = store.list_dir(&"a/".try_into()?)?;
    assert_eq!(list.keys(), &["a/file.txt".try_into()?]);
    assert_eq!(list.prefixes(), &["a/empty/".try_into()?]);

    // list_dir at a/empty/ should be empty (no files, no subdirectories)
    let list = store.list_dir(&"a/empty/".try_into()?)?;
    assert_eq!(list.keys(), &[]);
    assert_eq!(list.prefixes(), &[]);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
fn store_test_read_list() -> Result<(), Box<dyn Error>> {
    // Create a memory store and write test data
    let memory_store = Arc::new(MemoryStore::default());
    zarrs_storage::store_test::store_write(&memory_store)?;

    // Create a zip file from the memory store contents
    let tmp_dir = tempfile::TempDir::new()?;
    let zip_path = tmp_dir.path().join("test.zip");
    {
        let file = File::create(&zip_path)?;
        let mut zip = zip::ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        // Get all keys from memory store and write to zip
        let keys = memory_store.list()?;
        for key in keys {
            if let Some(data) = memory_store.get(&key)? {
                zip.start_file::<&str, ()>(key.as_str(), options)?;
                zip.write_all(&data)?;
            }
        }
        zip.finish()?;
    }

    // Create zip storage adapter
    let fs_store = FilesystemStore::new(tmp_dir.path())?;
    let zip_store = Arc::new(ZipStorageAdapter::new(
        fs_store.into(),
        StoreKey::new("test.zip")?,
    )?);

    // Run the store tests
    zarrs_storage::store_test::store_read(&zip_store)?;
    zarrs_storage::store_test::store_list(&zip_store)?;

    Ok(())
}
