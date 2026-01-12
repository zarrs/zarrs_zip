#![allow(missing_docs)]

use std::sync::Arc;

use object_store::http::HttpBuilder;
use zarrs::{
    array::Array,
    node::Node,
    storage::AsyncReadableStorageTraits,
};
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::StoreKey;
use zarrs_zip::ZipStorageAdapter;

const ARRAY_PATH: &str = "/";

async fn read_array_from_store<TStorage: AsyncReadableStorageTraits + 'static>(
    array: Array<TStorage>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read the whole array
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&array.subset_all())
        .await?;
    println!("The whole array is:\n{data_all}\n");

    // Read a chunk back from the store
    let chunk_indices = vec![1, 0];
    let data_chunk = array
        .async_retrieve_chunk_ndarray::<f32>(&chunk_indices)
        .await?;
    println!("Chunk [1,0] is:\n{data_chunk}\n");

    // Read the central 4x2 subset of the array
    let subset_4x2 = zarrs::array_subset::ArraySubset::new_with_ranges(&[2..6, 3..5]);
    let data_4x2 = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_4x2)
        .await?;
    println!("The middle 4x2 subset is:\n{data_4x2}\n");

    Ok(())
}

async fn zip_array_read_async() -> Result<(), Box<dyn std::error::Error>> {
    // Create an HTTP object store pointing to the raw GitHub URL
    let store = HttpBuilder::new()
        .with_url("https://github.com/zarrs/zarrs_zip/raw/refs/heads/main/tests/")
        .build()?;
    let store = Arc::new(AsyncObjectStore::new(store));

    println!("Fetching remote zip from GitHub...\n");

    // Create the zip storage adapter asynchronously
    let zip_key = StoreKey::new("example.zip")?;
    let store = Arc::new(ZipStorageAdapter::new_async(store, zip_key).await?);

    // Open the array
    let array = Array::async_open(store.clone(), ARRAY_PATH).await?;

    println!(
        "The array metadata is:\n{}\n",
        array.metadata().to_string_pretty()
    );

    // Read array data
    read_array_from_store(array).await?;

    // Show the hierarchy
    let node = Node::async_open(store, "/").await.unwrap();
    let tree = node.hierarchy_tree();
    println!("The Zarr hierarchy tree is:\n{}", tree);

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = zip_array_read_async().await {
        println!("{:?}", err);
    }
}
