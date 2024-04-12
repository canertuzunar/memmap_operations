#![allow(unused)]
use std::{
    ffi::FromBytesUntilNulError,
    fs::{File, OpenOptions},
    io::{Error, Read, Seek},
};

use memmap2::{MmapMut, MmapOptions, RemapOptions};

use serde::{Deserialize, Serialize};

const FOOTER_SIZE: u64 = 8;

#[derive(Serialize, Deserialize, Debug)]
struct IndexBlock {
    index_block: Vec<IndexData>,
}

#[derive(Serialize, Deserialize, Debug)]
struct IndexData {
    index_key: String,
    offset: u64,
    value_length: u64,
}

#[derive(Debug)]
struct Footer {
    index_block_offset: u64,
}

impl Footer {
    fn to_bytes(&self) -> [u8; 8] {
        self.index_block_offset.to_le_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 8];
        byte_array.copy_from_slice(&bytes[0..8]);
        let index_block_offset = u64::from_le_bytes(byte_array);
        println!("{:?}", bytes);
        Footer { index_block_offset }
    }
}

impl IndexBlock {
    fn new(index_data: IndexData) -> Self {
        let index_block = vec![index_data];
        Self { index_block }
    }

    fn append(&mut self, index: IndexData) {
        self.index_block.push(index)
    }

    fn get_serialized(&self) -> Vec<u8> {
        let serialized_block = serde_json::to_vec(&self).unwrap();
        serialized_block
    }

    fn get_deserialized(iblock: &Vec<u8>) -> Self {
        println!("{:?}", String::from_utf8_lossy(iblock));
        let deserialized: IndexBlock = serde_json::from_slice(&iblock).expect("deserialized error");

        deserialized
    }

    fn get_value_offset(&self) -> &u64 {
        &self.index_block[1].offset
    }
}

impl IndexData {
    fn new(index_key: String, offset: u64, value_length: u64) -> Self {
        Self {
            index_key,
            offset,
            value_length,
        }
    }
}

fn main() {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open("memmap")
        .expect("file cannot created");

    let remap_options = RemapOptions::new();

    let file_length = file.metadata().unwrap().len();

    let key = "key1";
    let value = "value1";

    let mut mmap = unsafe {
        MmapOptions::new()
            .map_mut(&file)
            .expect("failed to create map of file")
    };

    write_mmap(&mut file, &mut mmap, key, value);
    write_mmap(&mut file, &mut mmap, "key2", "value2");
    write_mmap(&mut file, &mut mmap, "key3", "value3");
    read_mmap(mmap, "key1")
}

fn read_mmap(mmap: MmapMut, key: &str) {
    // read index block offset from footer
    let mut mmap_len = &mmap.len();

    let footer_buf = &mmap[mmap_len - FOOTER_SIZE as usize..*mmap_len];

    let footer = Footer::from_bytes(footer_buf);

    println!("index block offset {}", footer.index_block_offset as usize);

    let end_on_index_block = mmap_len - (FOOTER_SIZE as usize);

    println!("{}, {}", footer.index_block_offset, end_on_index_block);
    let mut index_block = &mmap[(footer.index_block_offset as usize)..end_on_index_block];

    let ib = IndexBlock::get_deserialized(&index_block.to_vec());

    println!(
        "index block data is {}",
        String::from_utf8_lossy(&index_block)
    );

    let idata = &ib.index_block[0];

    println!(
        "value offset is {} and length is {}",
        idata.offset, idata.value_length
    );

    let val = &mmap[(idata.offset as usize) - 1..];

    println!("value is {}", String::from_utf8_lossy(&val));
}

fn write_mmap(mut file: &mut File, mut mmap: &mut MmapMut, key: &str, value: &str) {
    let remap_options = RemapOptions::new();
    let data = format!("{}:{}", key, value);
    let mut mmap_len = mmap.len();

    if mmap_len >= data.len() {
        mmap[mmap_len - data.len()..].copy_from_slice(data.as_bytes());
    } else {
        println!(
            "remap mmap from {} to {}",
            mmap_len,
            (mmap_len + data.len())
        );
        unsafe {
            mmap.remap(
                mmap_len + data.len(),
                RemapOptions::may_move(remap_options, true),
            )
            .expect("remap failed: ");
        }
        mmap_len = mmap.len();
        file.set_len(mmap_len as u64);
        println!("mmap len is {}", mmap_len);
        mmap[mmap_len - data.len()..].copy_from_slice(data.as_bytes());
    }

    mmap_len = mmap.len();

    //get index block offset
    let index_block_offset = mmap_len as u64;

    //create index data
    let value_offset = mmap_len - value.len();
    let idata = IndexData::new(String::from(key), value_offset as u64, value.len() as u64);
    println!("current index data is {:?}", idata);

    //Index data must append to iblock if IndexBlock exist
    // let check_footer = &mmap[mmap_len - FOOTER_SIZE];

    let iblock = IndexBlock::new(idata);

    mmap_len = mmap.len();
    file.set_len((mmap_len + iblock.get_serialized().len()) as u64);
    unsafe {
        mmap.remap(
            mmap_len + iblock.get_serialized().len() as usize,
            RemapOptions::may_move(remap_options, true),
        )
        .expect("remap error");
    }
    mmap_len = mmap.len();
    mmap[mmap_len - iblock.get_serialized().len()..].copy_from_slice(&&iblock.get_serialized());

    //create footer with index block offset information
    let footer = Footer { index_block_offset };

    //move to cursor end of the file
    println!("footer bytes {:?}", &footer.to_bytes());

    //write footer the mmap
    mmap_len = mmap.len();

    file.set_len((mmap_len + footer.to_bytes().len()) as u64);
    unsafe {
        mmap.remap(
            (mmap_len + footer.to_bytes().len()) as usize,
            RemapOptions::may_move(remap_options, true),
        )
        .expect("remap error");
    }
    mmap_len = mmap.len();
    mmap[mmap_len - &footer.to_bytes().len()..].copy_from_slice(&footer.to_bytes()[..]);
    //flush data<footer> to disk
    // mmap.flush().expect("data block flush failed")
}

// fn check_index_block(mmap: MmapMut) -> (IndexBlock, bool)  {
//    let mmap_len = mmap.len();

//    let footer_start_offset = mmap_len - FOOTER_SIZE as usize;
//    let footer_bytes = &mmap[footer_start_offset..];
//    let iblock = match Footer::from_bytes(footer_bytes) {
//       Ok(footer) => {

//       },
//       Err(e) => {
//          return false
//       }
//    }
// }
