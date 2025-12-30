// mod lib;
mod constants;
mod types;
mod buffer;
mod error;
mod record;

use axum::{
    routing::get,
    Router,
};

// use crate::
// use rust_api::Database;
use crate::record::Encoder::{self, decode_record};
use crate::types::{Column, DataType, DataTypeValue};
// use rust_api::{Database, record::Encoder::decode_record, types::{Column, DataType, DataTypeValue}};
// use rust_api::{create_database, open_database, read_page};

use std::{cell::{Cell, RefCell}, path::Path, rc::Rc, io::Error};

use crate::types::{DbResult, PageHeader};
// use rust_api::types::{DbResult, PageHeader};

use crate::record::Encoder::{encode_record};
// use rust_api::record::Encoder::{encode_record};
// use crate::{types::{DbResult, PageHeader}};

#[tokio::main]
async fn main() -> DbResult<()> {
    // let app = Router::new()
    //     .route("/", get(|| async { "Hello World!" }))
    //     .route("/test", get(|| async { "tes world" }));

    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    // axum::serve(listener, app).await.unwrap();
    let path = Path::new("./data/test.db");
    // let database = create_database(path)?;
    // let mut db = Database::new(path)?;
    let mut db = Database::open_database(path)?;

    /**
     * TEST 1 Test Buffer Pool
     */

    // print!("The file header struct is: {:#?}", file_header);
    // println!("got passed here");
    // let page_1_bytes = db.read_page(1)?;
    // let page_2_bytes = db.read_page(2)?;


    // let table_header = PageHeader::from_bytes(&page_1_bytes);
    // let column_header = PageHeader::from_bytes(&page_2_bytes);

    // print!("Table header is: {:#?}", table_header);
    // print!("Table column is: {:#?}", column_header);
    
    // let page_2_bytes = db.read_page(2)?;
    // let column_header = PageHeader::from_bytes(&page_2_bytes);

    // print!("Table column second read is: {:#?}", column_header);

    /**
     * 
     * TEST 2 Record Encoding/Decoding
    */

    // let test_schema = TestSchema {
    //     id: 1,
    //     name: String::from("Sempo space x"),
    //     active: true
    // };

    // let columns = vec![
    //     Column {
    //         table_id: 1,
    //         column_id: 2,
    //         name: String::from("id"),
    //         data_type: DataType::UInt32,
    //         nullable: false,
    //         position: 2,
    //         is_primary: false,
    //         max_length: None, 
    //     },

    //     Column {
    //         table_id: 1,
    //         column_id: 2,
    //         name: String::from("username"),
    //         data_type: DataType::String,
    //         nullable: true,
    //         position: 2,
    //         is_primary: false,
    //         max_length: Some(6), 
    //     },
        
    //     Column {
    //         table_id: 1,
    //         column_id: 2,
    //         name: String::from("active"),
    //         data_type: DataType::Bool,
    //         nullable: false,
    //         position: 2,
    //         is_primary: false,
    //         max_length: None, 
    //     }
    // ];

    

    // let values = vec![
    //     DataTypeValue::UInt32(1022),
    //     // DataTypeValue::Null,
    //     DataTypeValue::String(String::from("active")),
    //     DataTypeValue::Bool(false),
    // ];

    // let encoded_bytes = encode_record(&columns, &values)?;
    // let decoded_record = decode_record(columns, &encoded_bytes);

    // println!("The passed data record is {values:?}");
    // println!("The Decoded data record is {decoded_record:?}");



    /**
     * 
     * TEST 3 Inserting/Reading Record
    */

    let column_1 = vec![
        Column {
            table_id: 1,
            column_id: 2,
            name: String::from("id"),
            data_type: DataType::UInt32,
            nullable: false,
            position: 2,
            is_primary: false,
            max_length: None, 
        },
    ];

    let column_2 = vec![
        Column {
            table_id: 1,
            column_id: 2,
            name: String::from("username"),
            data_type: DataType::String,
            nullable: true,
            position: 2,
            is_primary: false,
            max_length: Some(6), 
        }
    ];
    
    let column_3 = vec![
        Column {
            table_id: 1,
            column_id: 2,
            name: String::from("active"),
            data_type: DataType::Bool,
            nullable: false,
            position: 2,
            is_primary: false,
            max_length: None, 
        }
    ];

    

    let values_1 = vec![
        DataTypeValue::UInt32(1022),
    ];

    let values_2 = vec![
        DataTypeValue::String(String::from("active")),
    ];

    let values_3 = vec![
        DataTypeValue::Bool(false),
    ];

    // let encoded_bytes_1 = encode_record(&column_1, &values_1)?;
    // let encoded_bytes_2 = encode_record(&column_2, &values_2)?;
    // let encoded_bytes_3 = encode_record(&column_3, &values_3)?;

    // let mut empty_data_page_bytes = Database::create_page(PageType::Data)?;
    let mut empty_data_page_bytes = db.read_page(4)?;
    // let slot_index_1 = Encoder::insert_record(&mut empty_data_page_bytes, &encoded_bytes_1)?;
    // let slot_index_2 = Encoder::insert_record(&mut empty_data_page_bytes, &encoded_bytes_2)?;
    // let slot_index_3 = Encoder::insert_record(&mut empty_data_page_bytes, &encoded_bytes_3)?;
    // Encoder::delete_record(&mut empty_data_page_bytes, 1)?;
    
    // db.write_page(4, &empty_data_page_bytes)?;

    // Database::flush_page(&db.file, 4, &empty_data_page_bytes)?;

    // let slot_data_1 = Encoder::read_record(&empty_data_page_bytes, 0)?;
    let slot_data_2 = Encoder::read_record(&empty_data_page_bytes, 1)?;
    let slot_data_3 = Encoder::read_record(&empty_data_page_bytes, 2)?;
    // let slot_data_1 = Encoder::read_record(&empty_data_page_bytes, slot_index_1)?;
    // let slot_data_2 = Encoder::read_record(&empty_data_page_bytes, slot_index_2)?;
    // let slot_data_3 = Encoder::read_record(&empty_data_page_bytes, slot_index_3)?;

    // let decoded_slot_record_1 = decode_record(column_1, &slot_data_1)?;
    let decoded_slot_record_2 = decode_record(column_2, &slot_data_2)?;
    let decoded_slot_record_3 = decode_record(column_3, &slot_data_3)?;

    // println!("The Decoded data record for slot 1 is {decoded_slot_record_1:?}");
    println!("The Decoded data record for slot 2 is {decoded_slot_record_2:?}");
    println!("The Decoded data record for slot 3 is {decoded_slot_record_3:?}");



















    // let a: Rc<Cell<u8>> = Rc::new(Cell::new(5));

    // a.set(1);

    // let b = a.clone();

    // b.set(3);

    // println!("{}", a.get());

    // println!("{:?}", b.get());

    // println!("a points to: {:p}", Rc::as_ptr(&a));
    // println!("b points to: {:p}", Rc::as_ptr(&b));

    Ok(())
}

use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write},
    // path::Path,
};

use constants::{MAGIC_NUMBER, PAGE_HEADER_SIZE, PAGE_SIZE};

use types::FileHeader;

use crate::{
    buffer::pool::BufferPool,
    types::{PageType},
};

pub type DbOpenResult = DbResult<Database>;

pub struct Database {
    file: File,
    buffer_pool: BufferPool,
    header: FileHeader,
}

impl Database {
    pub fn new(path: &Path) -> DbResult<Self> {
         // Todo: add auto create folder for db files if folder missing
        let file = OpenOptions::new()
         .read(true)
         .write(true)
         .create_new(true)
         .open(path)?;

     // self.buffer_pool = BufferPool::new();

     let mut writer = BufWriter::new(file);
    //  let mut writer = BufWriter::new(&self.file);

     // add magic number to database for uniqueness
     // // might not need this, let's see
     // writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

     //create headers
    let header = FileHeader::new();
    //  self.header = FileHeader::new();
     // let file_header = FileHeader::new();

     let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
     let column_page_header = PageHeader::new(None, None);

     writer.write_all(&header.to_bytes())?;
     writer.write_all(&table_page_header.to_bytes())?;
     writer.write_all(&column_page_header.to_bytes())?;

     writer.flush()?;

    //  writer
    let file = writer.into_inner()?;

     Ok(
        Self {
            file,
            buffer_pool: BufferPool::new(),
            header
        }
     )
    }

    pub fn read_page(&mut self , page_id: u32) -> DbResult<[u8; PAGE_SIZE as usize]> {
        // Try to read from buffer pool first 
        if let Some(page) = self.buffer_pool.find_page(page_id)? {
            println!("Read page with id from Buffer Pool: {:?}", page.page_id);
            page.last_accessed += 1;
            return Ok(page.data)
        }

        let mut reader = BufReader::new(&self.file);

        let file_size = reader.seek(SeekFrom::End(0))?;

        let mut buffer = [0u8; PAGE_SIZE as usize];
        // let mut buffer = [0u8; 4096];

        let offset = (page_id * PAGE_SIZE) as u64;

        println!("File size: {} bytes", file_size);
        println!("Trying to read at offset: {}", offset);

        reader.seek(SeekFrom::Start(offset))?;

        reader.read_exact(&mut buffer)?;

        //writes page to buffer for future read
        let mut empty_slot = self.buffer_pool.find_empty_slot()?;
        empty_slot.data = buffer;
        empty_slot.page_id = Some(page_id);

        Ok(buffer)
    }

    pub fn write_page(&mut self, page_id: u32, data: &[u8; PAGE_SIZE as usize]) -> DbResult<()> {
        
        if let Some(page) = self.buffer_pool.find_page(page_id)? {
            // finds page in buffer snd writes
            page.data = *data;
            page.is_dirty = true;
            page.pin_count -= 1;
            return Ok(())
        } else {
            // writes to disk and to buffer
            let mut writer = BufWriter::new(&self.file);
    
            let offset = (page_id * PAGE_SIZE) as u64;
    
            writer.seek(SeekFrom::Start(offset))?;
    
            writer.write_all(data)?;
    
            writer.flush()?;

            let empty_slot = self.buffer_pool.find_empty_slot()?;
            empty_slot.data = *data;
            empty_slot.page_id = Some(page_id);
            empty_slot.is_dirty = false;

    
            Ok(())
            
        }

    }

    pub fn create_page(page_type: PageType) -> DbResult<[u8; PAGE_SIZE as usize]> {
        let mut page = [0u8; PAGE_SIZE as usize];
        let sample_table_id = Some(1); // Todo: Use real table Id
        if let t = page_type {
            let page_header_bytes = PageHeader::new(Some(t), sample_table_id).to_header_bytes();
            page[0..16].copy_from_slice(&page_header_bytes);
        }
        Ok(page)
    }

    pub fn flush_all(&mut self, file: &mut File) -> DbResult<()> {
        // let mut buffer_pool = &self.buffer_pool;
        for entry in &mut self.buffer_pool.entries {
            if let Some(page_id) = entry.page_id{
                if entry.is_dirty {
                    // self.buffer_pool.flush_page(page_id, file);
                    Database::flush_page(file, page_id, &entry.data);
                    entry.is_dirty = false;
                }
            }
        }
        Ok(())
    }

    pub fn flush_page(file: &File, page_id: u32, data: &[u8; PAGE_SIZE as usize]) -> DbResult<()> {
        let mut writer = BufWriter::new(file);

        let offset = (page_id * PAGE_SIZE) as u64;

        writer.seek(SeekFrom::Start(offset))?;

        writer.write_all(data)?;

        writer.flush()?;
        
        Ok(())
    }

    // pub fn create_database (path: &Path) -> Result<BufWriter<File>> {
    // fn create_database(&mut self, path: &Path) -> Result<()> {
    //     // Todo: add auto create folder for db files if folder missing
    //     self.file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create_new(true)
    //         .open(path)?;

    //     // self.buffer_pool = BufferPool::new();

    //     let mut writer = BufWriter::new(&self.file);

    //     // add magic number to database for uniqueness
    //     // // might not need this, let's see
    //     // writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

    //     //create headers
    //     self.header = FileHeader::new();
    //     // let file_header = FileHeader::new();

    //     let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
    //     let column_page_header = PageHeader::new(None, None);

    //     writer.write_all(&self.header.to_bytes())?;
    //     writer.write_all(&table_page_header.to_bytes())?;
    //     writer.write_all(&column_page_header.to_bytes())?;

    //     writer.flush()?;

    //     Ok(()) // Todo: Handle error by creating a new instance of the file upon error with unwrap_or_default
    // }

    pub fn open_database(path: &Path) -> DbOpenResult {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut file_header = [0u8; PAGE_SIZE as usize];
        file.read_exact(&mut file_header)?;

        //confirm magic number matches what we expect
        if &file_header[..4] != MAGIC_NUMBER.to_le_bytes() {
            // magic number error
            panic!("Magic number don't match");
        }

        let file_header = FileHeader::from_bytes(&file_header);

        // file.seek(SeekFrom::Start((0)))?;

        // Ok((file_header, file))
        Ok(Self {
            file,
            buffer_pool: BufferPool::new(),
            header: file_header
        })
    }
}