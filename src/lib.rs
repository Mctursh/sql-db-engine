mod buffer;
mod constants;
mod error;
pub mod types;
pub mod record;

// use std::{
//     fs::{File, OpenOptions},
//     io::{BufRead, BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write},
//     path::Path,
// };

// use constants::{MAGIC_NUMBER, PAGE_HEADER_SIZE, PAGE_SIZE};

// use types::FileHeader;

// use crate::{
//     buffer::pool::BufferPool,
//     types::{DbResult, PageHeader, PageType},
// };

// pub type DbOpenResult = DbResult<Database>;

// pub struct Database {
//     file: File,
//     buffer_pool: BufferPool,
//     header: FileHeader,
// }

// impl Database {
//     pub fn new(path: &Path) -> DbResult<Self> {
//          // Todo: add auto create folder for db files if folder missing
//         let file = OpenOptions::new()
//          .read(true)
//          .write(true)
//          .create_new(true)
//          .open(path)?;

//      // self.buffer_pool = BufferPool::new();

//      let mut writer = BufWriter::new(file);
//     //  let mut writer = BufWriter::new(&self.file);

//      // add magic number to database for uniqueness
//      // // might not need this, let's see
//      // writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

//      //create headers
//     let header = FileHeader::new();
//     //  self.header = FileHeader::new();
//      // let file_header = FileHeader::new();

//      let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
//      let column_page_header = PageHeader::new(None, None);

//      writer.write_all(&header.to_bytes())?;
//      writer.write_all(&table_page_header.to_bytes())?;
//      writer.write_all(&column_page_header.to_bytes())?;

//      writer.flush()?;

//     //  writer
//     let file = writer.into_inner()?;

//      Ok(
//         Self {
//             file,
//             buffer_pool: BufferPool::new(),
//             header
//         }
//      )
//     }

//     pub fn read_page(&mut self , page_id: u32) -> DbResult<[u8; PAGE_SIZE as usize]> {
//         // Try to read from buffer pool first 
//         if let Some(page) = self.buffer_pool.find_page(page_id)? {
//             println!("Read page with id from Buffer Pool: {:?}", page.page_id);
//             page.last_accessed += 1;
//             return Ok(page.data)
//         }

//         let mut reader = BufReader::new(&self.file);

//         let file_size = reader.seek(SeekFrom::End(0))?;

//         let mut buffer = [0u8; PAGE_SIZE as usize];
//         // let mut buffer = [0u8; 4096];

//         let offset = (page_id * PAGE_SIZE) as u64;

//         println!("File size: {} bytes", file_size);
//         println!("Trying to read at offset: {}", offset);

//         reader.seek(SeekFrom::Start(offset))?;

//         reader.read_exact(&mut buffer)?;

//         //writes page to buffer for future read
//         let mut empty_slot = self.buffer_pool.find_empty_slot()?;
//         empty_slot.data = buffer;
//         empty_slot.page_id = Some(page_id);

//         Ok(buffer)
//     }

//     pub fn write_page(&mut self, page_id: u32, data: &[u8; PAGE_SIZE as usize]) -> DbResult<()> {
        
//         if let Some(page) = self.buffer_pool.find_page(page_id)? {
//             // finds page in buffer snd writes
//             page.data = *data;
//             page.is_dirty = true;
//             page.pin_count -= 1;
//             return Ok(())
//         } else {
//             // writes to disk and to buffer
//             let mut writer = BufWriter::new(&self.file);
    
//             let offset = (page_id * PAGE_SIZE) as u64;
    
//             writer.seek(SeekFrom::Start(offset))?;
    
//             writer.write_all(data)?;
    
//             writer.flush()?;

//             let empty_slot = self.buffer_pool.find_empty_slot()?;
//             empty_slot.data = *data;
//             empty_slot.page_id = Some(page_id);
//             empty_slot.is_dirty = false;

    
//             Ok(())
            
//         }

//     }

//     pub fn flush_all(&mut self, file: &mut File) -> DbResult<()> {
//         // let mut buffer_pool = &self.buffer_pool;
//         for entry in &mut self.buffer_pool.entries {
//             if let Some(page_id) = entry.page_id{
//                 if entry.is_dirty {
//                     // self.buffer_pool.flush_page(page_id, file);
//                     Database::flush_page(file, page_id, &entry.data);
//                     entry.is_dirty = false;
//                 }
//             }
//         }
//         Ok(())
//     }

//     pub fn flush_page(file: &File, page_id: u32, data: &[u8; PAGE_SIZE as usize]) -> DbResult<()> {
//         let mut writer = BufWriter::new(file);

//         let offset = (page_id * PAGE_SIZE) as u64;

//         writer.seek(SeekFrom::Start(offset))?;

//         writer.write_all(data)?;

//         writer.flush()?;
        
//         Ok(())
//     }

//     // pub fn create_database (path: &Path) -> Result<BufWriter<File>> {
//     // fn create_database(&mut self, path: &Path) -> Result<()> {
//     //     // Todo: add auto create folder for db files if folder missing
//     //     self.file = OpenOptions::new()
//     //         .read(true)
//     //         .write(true)
//     //         .create_new(true)
//     //         .open(path)?;

//     //     // self.buffer_pool = BufferPool::new();

//     //     let mut writer = BufWriter::new(&self.file);

//     //     // add magic number to database for uniqueness
//     //     // // might not need this, let's see
//     //     // writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

//     //     //create headers
//     //     self.header = FileHeader::new();
//     //     // let file_header = FileHeader::new();

//     //     let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
//     //     let column_page_header = PageHeader::new(None, None);

//     //     writer.write_all(&self.header.to_bytes())?;
//     //     writer.write_all(&table_page_header.to_bytes())?;
//     //     writer.write_all(&column_page_header.to_bytes())?;

//     //     writer.flush()?;

//     //     Ok(()) // Todo: Handle error by creating a new instance of the file upon error with unwrap_or_default
//     // }

//     pub fn open_database(path: &Path) -> DbOpenResult {
//         let mut file = OpenOptions::new().read(true).write(true).open(path)?;

//         let mut file_header = [0u8; PAGE_SIZE as usize];
//         file.read_exact(&mut file_header)?;

//         //confirm magic number matches what we expect
//         if &file_header[..4] != MAGIC_NUMBER.to_le_bytes() {
//             // magic number error
//             panic!("Magic number don't match");
//         }

//         let file_header = FileHeader::from_bytes(&file_header);

//         // file.seek(SeekFrom::Start((0)))?;

//         // Ok((file_header, file))
//         Ok(Self {
//             file,
//             buffer_pool: BufferPool::new(),
//             header: file_header
//         })
//     }
// }
