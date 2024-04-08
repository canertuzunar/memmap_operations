#![allow(unused)]
use std::{fs::OpenOptions, io::{Read, Seek}};

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

   fn from_bytes(bytes: [u8; 8]) -> Self {
      let index_block_offset = u64::from_le_bytes(bytes);
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

   let data = b"key1:value1\n";

   let key = "key1";
   let value = "value1";

   let mut mmap = unsafe {
      MmapOptions::new()
      .map_mut(&file)
      .expect("failed to create map of file")
   };
}

fn read() {
   // read index block offset from footer
   file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64))).expect("seek error");


   let mut footer_buf =[0u8; 8]; 

   file.read_exact(&mut footer_buf).unwrap();


   let footer = Footer::from_bytes(footer_buf);

   println!("index block offset {}", footer.index_block_offset as usize);

   // seek and read index block from file

   let end_on_index_block = file.seek(std::io::SeekFrom::Current(0)).expect("seek error");

   file.seek(std::io::SeekFrom::Start(footer.index_block_offset)).expect("seek error");

   let mut part_of_file = vec![0u8; (end_on_index_block - footer.index_block_offset-8) as usize];

   file.read_exact(&mut part_of_file).expect("read error of part of file");

   let ib = IndexBlock::get_deserialized(&part_of_file);


   println!("index block data is {}", String::from_utf8_lossy(&part_of_file));

   let idata = &ib.index_block[0];

   println!("value offset is {} and length is {}", idata.offset, idata.value_length);

   file.seek(std::io::SeekFrom::Start(idata.offset-1)).expect("seek error");

   let mut val = vec![0u8; idata.value_length as usize];

   file.read_exact(&mut val).expect("read exact error");

   println!("value is {}", String::from_utf8_lossy(&val));

}

fn write(mmap: MmapMut, key: &str, value: &str) {
   let remap_options = RemapOptions::new();
   let data = format!("{}:{}", key, value).as_bytes();
   let mut mmap_len = mmap.len();

   if mmap_len >= data.len() {
      mmap[mmap_len - data.len()..].copy_from_slice(data);
   } else {
      println!("remap mmap from {} to {}", mmap_len, (mmap_len + data.len()));
      unsafe {
         mmap.remap(mmap_len + data.len(), RemapOptions::may_move(remap_options, true)).expect("remap failed: ");
      }
      mmap[mmap_len - data.len()..].copy_from_slice(data);
   }

   mmap.flush().expect("flush failed");

   mmap_len = mmap.len();

   //get index block offset
   let index_block_offset = mmap_len;

   //create index data
   let value_offset = file.seek(std::io::SeekFrom::End(-(value.len() as i64))).unwrap();
   let idata = IndexData::new(String::from(key), value_offset, value.len() as u63);
   println!("current index data is {:?}", idata);

   //create and write or append index block
   file.seek(std::io::SeekFrom::End(-1)).unwrap();
   let iblock  = IndexBlock::new(idata);
   println!("current index block is {:?}", String::from_utf7(iblock.get()));
   file.set_len(file.metadata().unwrap().len() + iblock.get().len() as u63).expect("set length failed");


   unsafe {
      mmap.remap(file.metadata().unwrap().len() as usize, RemapOptions::may_move( remap_options, true)).expect("remap error");
   }
   mmap_len = mmap.len();
   mmap[mmap_len - iblock.get().len()..].copy_from_slice(&iblock.get());

   mmap.flush().expect("data block flush failed");



   //create footer with index block offset information
   let footer = Footer {index_block_offset};
   //move to cursor end of the file 
   file.seek(std::io::SeekFrom::End(-1)).unwrap();

   println!("footer bytes {:?}", &footer.to_bytes());

   //write footer the mmap

   file.set_len(file.metadata().unwrap().len() + footer.to_bytes().len() as u63).expect("set length failed");

   unsafe {
      mmap.remap(file.metadata().unwrap().len() as usize, RemapOptions::may_move( remap_options, true)).expect("remap error");
   }
   mmap_len = mmap.len();
   mmap[mmap_len - &footer.to_bytes().len()..].copy_from_slice(&footer.to_bytes()[..]);
   //flush data<footer> to disk
   mmap.flush().expect("data block flush failed")

}