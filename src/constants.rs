

pub const PAGE_SIZE: u32  = 4096;
pub const MAGIC_NUMBER: u32  =  0x47726F64; // Which means "Grod"
pub const FILE_FORMAT_VERSION: u8 = 1;
pub const NULL_PAGE: u32 = 0xFFFFFFFF;
pub const PAGE_HEADER_SIZE: usize = 16;
pub const FILE_HEADER_PAGE: u8 = 0;
pub const TABLES_CATALOG_PAGE: u8 = 1;
pub const COLUMNS_CATALOG_PAGE: u8 = 2;
pub const FIRST_DATA_PAGE: u32 = 3;
pub const MAX_STRING_LENGTH: u16 = 65535;

