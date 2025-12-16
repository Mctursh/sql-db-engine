use core::fmt;
use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::constants::{
    COLUMNS_CATALOG_PAGE, FILE_FORMAT_VERSION, FIRST_DATA_PAGE, MAGIC_NUMBER, NULL_PAGE, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_BYTE_SIZE, TABLES_CATALOG_PAGE
};

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    Data = 1,
    // NullPage = 2
    // INDEX_INTERNAL = 3,
    // INDEX_LEAF = 4,
    // Overflow = 5,

}
#[repr(u8)]
pub enum DataType {
    UInt32 = 1,
    Int32 = 2,
    UInt64 = 3,
    Int64 = 4,
    Bool = 5,
    String = 6
}

pub enum DataTypeValue {
    Null,
    UInt32(u32),
    Int32(i32),
    UInt64(u64),
    Int64(i64),
    Bool(bool),
    String(String)
}

pub struct Table {
    table_id: u32,
    name: String,
    first_page: u32,
    index_root_page: Option<u32>,
    auto_increment_counter: Option<u64>
}

impl Table {
    pub fn new () {
        // Self {
        //     table_id: 1,
        //     // name: 
        // }
    }
    fn to_bytes (&self) {}
    fn from_bytes (&self) {}
}

pub struct Column {
    table_id: u32,
    column_id: u32,
    name: String,
    pub data_type: DataType,
    nullable: bool,
    position: u8,
    is_primary: bool,
    max_length: Option<u16>
}

#[derive(Debug)]
pub struct DatabaseError {
    code: u16,
    message: String,
    // info: Option<>
}

impl Display for DatabaseError {
    fn fmt (&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Error {}, reason: {}", self.code, self.message)
    }
}

impl Error for DatabaseError {}
#[derive(Debug)]
pub struct FileHeader {
    magic: u32,
    version: u8,
    page_size: u32,
    page_count: u32,
    free_list_head: u32,
    tables_page: u8,
    columns_page: u8,
    next_table_id: u32,
    next_index_id: u32
}

impl FileHeader {
    pub fn new () -> Self {
        Self {
            magic: MAGIC_NUMBER,
            version: FILE_FORMAT_VERSION,
            page_size: PAGE_SIZE,
            page_count: FIRST_DATA_PAGE, // page count is same as first_data_page upon creation
            free_list_head: FIRST_DATA_PAGE, // first free page is same as first page data upon creation
            tables_page: TABLES_CATALOG_PAGE,
            columns_page: COLUMNS_CATALOG_PAGE,
            next_table_id: 1, // defaults to one because upon creation there isn't any table existing yet
            next_index_id: 1 // same as above
        }
    }

    pub fn to_bytes (&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        bytes.push(self.version);
        bytes.extend_from_slice(&self.page_size.to_le_bytes());
        bytes.extend_from_slice(&self.page_count.to_le_bytes());
        bytes.extend_from_slice(&self.free_list_head.to_le_bytes());
        bytes.push(self.tables_page);
        bytes.push(self.columns_page);
        bytes.extend_from_slice(&self.next_index_id.to_le_bytes());
        bytes.extend_from_slice(&self.next_table_id.to_le_bytes());

        bytes.resize(PAGE_SIZE as usize, 0); // fill the remaining space of the page to reserve for future changes.

        bytes
    }

    pub fn from_bytes (data: &[u8]) -> Self {
        Self {
            magic: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            version: data[4],
            page_size: u32::from_le_bytes([data[5], data[6], data[7], data[8]]),
            page_count: u32::from_le_bytes([data[9], data[10], data[11], data[12]]),
            free_list_head: u32::from_le_bytes([data[13], data[14], data[15], data[16]]),
            tables_page: data[17],
            columns_page: data[18],
            next_table_id: u32::from_le_bytes([data[19], data[20], data[21], data[22]]),
            next_index_id: u32::from_le_bytes([data[23], data[24], data[25], data[26]])
        }
    }
}
#[derive(Debug)]
pub struct PageHeader {
    page_type: PageType,
    pub record_count: u16, // amount of slots in the page(normal and tombstone slots)
    pub free_space_offset: u16, // The byte offset where the last record on the page ends
    next_page: u32,
    table_id: u32
}

impl PageHeader {
    // pub const SIZE: usize = 16;

    pub fn new (page_type: Option<PageType>, table_id: Option<u32>) -> Self {
        let mut used_page_type = PageType::Data; // defaults to data page

        let used_table_id = match page_type {
            Some(t) => {
                used_page_type = t;
                match table_id {
                    Some(id) => id,
                    None => NULL_PAGE 
                }
            },
            None => NULL_PAGE
        };

        Self {
            page_type: used_page_type,
            record_count: 0, // new page always have 0 record
            free_space_offset: 16, // because this header is always 16 bytes
            next_page: NULL_PAGE, // always null because it has no record and hence it does't overflow
            table_id: used_table_id
        }
        
    }

    pub fn to_bytes (&self) -> [u8; PAGE_SIZE as usize] {
        let mut bytes = [0u8; PAGE_SIZE as usize];
        bytes[0] = self.page_type as u8;
        bytes[1..3].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[3..5].copy_from_slice(&self.free_space_offset.to_le_bytes());
        bytes[5..9].copy_from_slice(&self.next_page.to_le_bytes());
        bytes[9..13].copy_from_slice(&self.table_id.to_le_bytes());

        bytes
    }

    pub fn from_bytes (data: &[u8; PAGE_SIZE as usize]) -> Self {
        Self {
            page_type: match data[0] {
                0 => PageType::Free,
                1 => PageType::Data,
                _ => PageType::Data // Todo: handle error for unmatch page type, defaults to data page for now.
            },
            record_count: u16::from_le_bytes([data[1], data[2]]),
            free_space_offset: u16::from_le_bytes([data[3], data[4]]),
            next_page: u32::from_le_bytes([data[5], data[6], data[7], data[8]]),
            table_id: u32::from_le_bytes([data[9], data[10], data[11], data[12]])
        }
    }

    pub fn calculate_free_space_offset (&self) {

    }
}


pub struct Slot {
    pub offset: u16,
    pub length: u16,
}

impl Slot {
    
    pub fn from_bytes (bytes: [u8; SLOT_BYTE_SIZE as usize]) -> Self {
        Self {
            offset: u16::from_le_bytes([bytes[0], bytes[1]]),
            length: u16::from_le_bytes([bytes[2], bytes[3]])
        }
    }

    pub fn to_bytes(length: u16, offset: u16) -> [u8; SLOT_BYTE_SIZE as usize] {
        let mut bytes = [0u8; SLOT_BYTE_SIZE as usize];
        bytes[0..2].copy_from_slice(&offset.to_le_bytes());
        bytes[2..4].copy_from_slice(&length.to_le_bytes());

        bytes
    }
    //     bytes[0..2].copy_from_slice(u16::from_le_bytes([data[3], data[4]]));
    // }
}




