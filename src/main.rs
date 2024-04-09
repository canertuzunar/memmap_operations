#![allow(unused)]
use std::{fs::{File, OpenOptions}, io::{Read, Seek}};

use memmap2::{MmapMut, MmapOptions, RemapOptions};

use serde::{Deserialize, Serialize};


const FOOTER_SIZE: u64 = 8;

#[derive(Serialize, Deserialize, Debug)]
struct IndexBlock {
   index_block: Vec<IndexData>
}

#[derive(Serialize, Deserialize, Debug)]
struct IndexData {
    index_key: String,
    offset: u64,
    value_length: u64
}

#[derive(Debug)] 
struct Footer {
   index_block_offset: u64
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
      Self {
         index_block
      }
   }

   fn append(&mut self, index: IndexData) {
      self.index_block.push(index)
   } 

   fn get_serialized(&self) -> Vec<u8> {
      let serialized_block = serde_json::to_vec(&self).unwrap();
      serialized_block
   }

   fn get_deserialized(iblock: &Vec<u8>) -> Self {
      println!("dd{:?}", String::from_utf8_lossy(iblock));
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
         value_length
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

   // write_mmap(&mut file, &mut mmap, key, value);
   read_mmap(mmap, "key1")

}

fn read_mmap(mmap: MmapMut, key: &str) {
   // read index block offset from footer
   let mut mmap_len = &mmap.len(); 

   let footer_buf = &mmap[mmap_len - FOOTER_SIZE as usize..*mmap_len];
   // file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64))).expect("seek error");


   // let mut footer_buf =[0u8; 8]; 

   // file.read_exact(&mut footer_buf).unwrap();


   let footer = Footer::from_bytes(footer_buf);

   println!("index block offset {}", footer.index_block_offset as usize);

   // seek and read index block from file

   let end_on_index_block = mmap_len - (FOOTER_SIZE as usize);

   // file.seek(std::io::SeekFrom::Start(footer.index_block_offset)).expect("seek error");

   // let mut part_of_file = vec![0u8; (end_on_index_block - footer.index_block_offset as usize-8) as usize];
   println!("{}, {}", footer.index_block_offset, end_on_index_block);
   // let mut part_of_file:&[u8]= &mmap[end_on_index_block - footer.index_block_offset as usize -55..end_on_index_block - footer.index_block_offset as usize +11];
   let mut part_of_file= &mmap[(footer.index_block_offset as usize)..end_on_index_block];
   // file.read_exact(&mut part_of_file).expect("read error of part of file");

   let ib = IndexBlock::get_deserialized(&part_of_file.to_vec());


   println!("index block data is {}", String::from_utf8_lossy(&part_of_file));

   let idata = &ib.index_block[0];

   println!("value offset is {} and length is {}", idata.offset, idata.value_length);

   // file.seek(std::io::SeekFrom::Start(idata.offset-1)).expect("seek error");

   // let mut val = vec![0u8; idata.value_length as usize];

   // file.read_exact(&mut val).expect("read exact error");

   let val = &mmap[(idata.offset as usize)-1..];

   println!("value is {}", String::from_utf8_lossy(&val));

}

fn write_mmap(mut file: &mut File, mut mmap: &mut MmapMut, key: &str, value: &str) {
   let remap_options = RemapOptions::new();
   let data = format!("{}:{}", key, value);
   let mut mmap_len = mmap.len();

   if mmap_len >= data.len() {
      mmap[mmap_len - data.len()..].copy_from_slice(data.as_bytes());
   } else {
      println!("remap mmap from {} to {}", mmap_len, (mmap_len + data.len()));
      unsafe {
         mmap.remap(mmap_len + data.len(), RemapOptions::may_move(remap_options, true)).expect("remap failed: ");
      }
      mmap_len = mmap.len();
      file.set_len(mmap_len as u64);
      println!("mmap len is {}", mmap_len);
      mmap[mmap_len - data.len()..].copy_from_slice(data.as_bytes());
   }

   println!("code is here");
   mmap_len = mmap.len();

   //get index block offset
   let index_block_offset = mmap_len as u64;

   //create index data
   let value_offset = mmap_len - value.len();
   let idata = IndexData::new(String::from(key), value_offset as u64, value.len() as u64);
   println!("current index data is {:?}", idata);

   //create and write or append index block
   // file.seek(std::io::SeekFrom::End(-1)).unwrap();
   let iblock  = IndexBlock::new(idata);
   // println!("current index block is {:?}", String::from_utf8(iblock.get()));
   // file.set_len(file.metadata().unwrap().len() + iblock.get().len() as u63).expect("set length failed");

   mmap_len = mmap.len();
   file.set_len((mmap_len + iblock.get_serialized().len()) as u64);
   unsafe {
      mmap.remap(mmap_len + iblock.get_serialized().len() as usize, RemapOptions::may_move( remap_options, true)).expect("remap error");
   }
   mmap_len = mmap.len();
   mmap[mmap_len - iblock.get_serialized().len()..].copy_from_slice(&&iblock.get_serialized());




   //create footer with index block offset information
   let footer = Footer {index_block_offset};
   //move to cursor end of the file 
   // file.seek(std::io::SeekFrom::End(-1)).unwrap();

   println!("footer bytes {:?}", &footer.to_bytes());

   //write footer the mmap

   // file.set_len(file.metadata().unwrap().len() + footer.to_bytes().len() as u63).expect("set length failed");

   mmap_len = mmap.len();

   file.set_len((mmap_len + footer.to_bytes().len()) as u64);
   unsafe {
      mmap.remap((mmap_len + footer.to_bytes().len()) as usize, RemapOptions::may_move( remap_options, true)).expect("remap error");
   }
   mmap_len = mmap.len();
   mmap[mmap_len - &footer.to_bytes().len()..].copy_from_slice(&footer.to_bytes()[..]);
   //flush data<footer> to disk
   mmap.flush().expect("data block flush failed")

}