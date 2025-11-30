mod constants;
mod types;

use std::{fs::{File, OpenOptions}, io::{BufRead, BufReader, BufWriter, Read, Result, Write}, path::Path};

use constants::{
    PAGE_SIZE,
    MAGIC_NUMBER
};

use types::{
    FileHeader
};

use crate::types::{PageHeader, PageType};


pub fn read_page (file: File, page_id: u32) -> Result<[u8; 4096]> {
    // let db_file = file::open("data.bin")?;
    let mut reader = BufReader::new(file);

    let mut buffer = [0u8; PAGE_SIZE as usize]; 
    // let mut buffer = [0u8; 4096]; 

    reader.read_exact(&mut buffer)?;

    Ok(buffer)

    // let offset = page_id * PAGE_SIZE;
}

pub fn create_database (path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;

    let mut writer = BufWriter::new(file);
    
    // add magic number to database for uniqueness
    // might not need this, let's see
    writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

    //create headers
    let file_header = FileHeader::new();

    let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
    let column_page_header = PageHeader::new(None, None);

    writer.write_all(&file_header.to_bytes())?;

    

    Ok(writer)

}