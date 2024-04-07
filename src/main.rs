use std::{fs::OpenOptions, io::{Read, Seek}};

use memmap2::{MmapMut, MmapOptions};


const FOOTER_SIZE: u64 = 8;

#[derive(Debug)]
struct IndexBlock {
   index_block: Vec<IndexData>
}

#[derive(Debug)]
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

   fn get(&self) -> Vec<u8> {
      let mut bytes = Vec::new();
      for data in &self.index_block {
          bytes.extend_from_slice(data.index_key.as_bytes());
          bytes.push(b':');
          bytes.extend_from_slice(&data.offset.to_le_bytes());
          bytes.push(b':');
          bytes.extend_from_slice(&data.value_length.to_le_bytes());
          bytes.push(b'\n');
      }
      bytes
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

   let file_length = file.metadata().unwrap().len();

   let data = b"key1:value1\n";

   let key = "key1";
   let value = "value1";

   if file_length == 0 {
      file.set_len(data.len() as u64).expect("set length failed");
   } else {
      file.seek(std::io::SeekFrom::End(0)).expect("Seek failed");
      file.set_len(file.metadata().unwrap().len() + data.len() as u64).expect("set length failed");
   }

   let mut mmap = unsafe {
      MmapOptions::new()
      .map_mut(&file)
      .expect("failed to create map of file")
   };

   // file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64))).unwrap();

   // let mut footer_buf =[0u8; 8]; 

   // file.read_exact(&mut footer_buf).unwrap();

   // let footer = Footer::from_bytes(footer_buf);

   // println!("index block offset {}", footer.index_block_offset);



   let mut mmap_len = mmap.len();

   if mmap_len >= data.len() {
      mmap[mmap_len - data.len()..].copy_from_slice(b"key1:value1\n");
   } else {
      panic!("File is too small to write data");
   }

   mmap.flush().expect("flush failed");

   mmap_len = mmap.len();
   //create index data
   let value_offset = file.seek(std::io::SeekFrom::End(-(value.len() as i64))).unwrap();
   let idata = IndexData::new(String::from(key), value_offset, value.len() as u64);

   //create and write or append index block
   file.seek(std::io::SeekFrom::End(0)).unwrap();
   let iblock  = IndexBlock::new(idata);
   println!("{} {}", file.metadata().unwrap().len(), iblock.get().len());
   file.set_len(file.metadata().unwrap().len() + iblock.get().len() as u64).expect("set length failed");
   println!("{}", file.metadata().unwrap().len());
   mmap[mmap_len - iblock.get().len()..].copy_from_slice(&iblock.get());

   mmap.flush().expect("data block flush failed");

   mmap_len = mmap.len();
   let index_block_offset = file.seek(std::io::SeekFrom::End(0)).unwrap();

   let footer = Footer {index_block_offset};

   file.seek(std::io::SeekFrom::End(0)).unwrap();

   println!("footer bytes {:?}", &footer.to_bytes());

   mmap[mmap_len - &footer.to_bytes().len()..].copy_from_slice(&footer.to_bytes()[..]);


   file.set_len(file.metadata().unwrap().len() + footer.to_bytes().len() as u64).expect("set length failed");

   mmap.flush().expect("data block flush failed");
   

}

fn get_mmap_len(mmap: &MmapMut) -> usize {
   mmap.len()
}

fn write() {}

fn read() {}
