use std::io::Error;

#[derive(Debug)]
pub enum DbError {
    UnexpectedToken { token: String, pos: usize },
    UnterminatedString,
    InvalidNumber { value: String },

    TableNotFound { name: String },
    TableExists { name: String },
    ColumnNotFound { column: String, table: String },
    TypeMismatch { expected: String, actual: String },

    NotNull { column: String },
    DuplicateKey { value: String },
    StringTooLong { max: usize },

    PageFull { page_id: u32 },
    CorruptPage { page_id: u32 },
    Io(Error)
    // SYNTAX_UNEXPECTED_TOKEN,
    // SYNTAX_UNTERMINATED_STRING,
    // SYNTAX_INVALID_NUMBER,
    // SCHEMA_TABLE_NOT_FOUND,
    // SCHEMA_TABLE_EXISTS,
    // SCHEMA_COLUMN_NOT_FOUND,
    // SCHEMA_TYPE_MISMATCH,
    // CONSTRAINT_NOT_NULL,
    // CONSTRAINT_DUPLICATE_KEY,
    // CONSTRAINT_STRING_TOO_LONG,
    // STORAGE_PAGE_FULL,
    // STORAGE_CORRUPT_PAGE,
    // STORAGE_DISK_ERROR,
    // INTERNAL_ERROR
}

impl From<Error> for DbError {
    fn from (err: Error) -> Self {
        DbError::Io(err)
    }
}
