mod constants;
mod types;
mod error;

use std::{fs::{File, OpenOptions}, io::{BufRead, BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write}, path::Path};

use constants::{
    PAGE_SIZE,
    MAGIC_NUMBER,
    PAGE_HEADER_SIZE
};

use types::{
    FileHeader
};

use crate::types::{PageHeader, PageType};

pub type DbOpenResult = Result<(FileHeader, File)>; 


pub fn read_page (file: &File, page_id: u32) -> Result<[u8; PAGE_SIZE as usize]> {
    // let db_file = file::open("data.bin")?;
    let mut reader = BufReader::new(file);

    let file_size = reader.seek(SeekFrom::End(0))?;

    let mut buffer = [0u8; PAGE_SIZE as usize]; 
    // let mut buffer = [0u8; 4096]; 
    
    let offset = (page_id * PAGE_SIZE) as u64;

    println!("File size: {} bytes", file_size);
    println!("Trying to read at offset: {}", offset);

    reader.seek(SeekFrom::Start(offset))?;

    reader.read_exact(&mut buffer)?;

    Ok(buffer)

}

pub fn write_page (file: &File, page_id: u32, data: &[u8; PAGE_SIZE as usize]) -> Result<()> {
    let mut writer = BufWriter::new(file);

    let offset = (page_id * PAGE_SIZE) as u64;

    writer.seek(SeekFrom::Start(offset))?;

    writer.write_all(data)?;

    writer.flush()?;

    Ok(())
}

// pub fn create_database (path: &Path) -> Result<BufWriter<File>> {
pub fn create_database (path: &Path) -> Result<File> {
    // Todo: add auto create folder for db files if folder missing
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;

    let mut writer = BufWriter::new(file);
    
    // add magic number to database for uniqueness
    // // might not need this, let's see
    // writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

    //create headers
    let file_header = FileHeader::new();

    let table_page_header = PageHeader::new(None, None); // passing None creates a system data page header
    let column_page_header = PageHeader::new(None, None);

    writer.write_all(&file_header.to_bytes())?;
    writer.write_all(&table_page_header.to_bytes())?;
    writer.write_all(&column_page_header.to_bytes())?;

    writer.flush()?;

    Ok(writer.into_inner().unwrap()) // Todo: Handle error by creating a new instance of the file upon error with unwrap_or_default

}


pub fn open_database(path: &Path) -> DbOpenResult {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;

    let mut file_header = [0u8; PAGE_SIZE as usize];
    file.read_exact(&mut file_header)?;

    //confirm magic number matches what we expect
    if &file_header[..4] != MAGIC_NUMBER.to_le_bytes() {
        // magic number error
        panic!("Magic number don't match");
    }

    let file_header = FileHeader::from_bytes(&file_header);

    // file.seek(SeekFrom::Start((0)))?;

    Ok((
        file_header,
        file
    ))
}