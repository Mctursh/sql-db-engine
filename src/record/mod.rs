

pub mod encoder {
    use crate::{constants::{BIT_DISCRIMINATOR, BITS_PER_BYTE, STRING_LENGTH_SIZE}, error::DbError, types::{Column, DataType, DataTypeValue}};

    type EncoderResult<T> = Result<T, DbError>;

    pub fn encoder (columns: Vec<Column>, values: Vec<DataTypeValue>) -> EncoderResult<Vec<u8>> {
        if columns.len() != values.len() {
            // TODO; use appropriate error
            return Err(DbError::UnterminatedString)
        }

        //TODO: add validation for encoding
        let mut encoded_bytes = get_bitmap(&values)?;

        for index in 0..values.len() {
            match &values[index] {
                DataTypeValue::UInt32(v) => {
                    encoded_bytes.extend_from_slice(&v.to_le_bytes());
                },
                DataTypeValue::Int32(v) => {
                    encoded_bytes.extend_from_slice(&v.to_le_bytes());
                },
                DataTypeValue::UInt64(v) => {
                    encoded_bytes.extend_from_slice(&v.to_le_bytes());
                },
                DataTypeValue::Int64(v) => {
                    encoded_bytes.extend_from_slice(&v.to_le_bytes());
                },
                DataTypeValue::Bool(v) => {
                    if *v {
                        encoded_bytes.push(1);
                    } else {
                        encoded_bytes.push(0);
                    }
                },
                DataTypeValue::String(v) => {
                    let string_length_byte: [u8; STRING_LENGTH_SIZE] = (v.len() as u16).to_le_bytes();
                    let str_bytes = v.as_bytes();
                    encoded_bytes.extend_from_slice(&string_length_byte);
                    encoded_bytes.extend_from_slice(&str_bytes);
                },
                _ => {}
            }
        }
        
        Ok(encoded_bytes)
    }

    pub fn decode_record () {}

    fn get_bitmap (values: &Vec<DataTypeValue>) -> EncoderResult<Vec<u8>> {
        let bitmap_size  = (values.len() + BIT_DISCRIMINATOR) / BITS_PER_BYTE;
        let mut null_bit_map: Vec<u8> = vec![0u8; bitmap_size];

        for (index, value) in values.iter().enumerate() {
            match value {
                DataTypeValue::Null => {
                    let byte_index = index / BITS_PER_BYTE;
                    let bit_offset = index % BITS_PER_BYTE;
                    null_bit_map[byte_index] |= 1 << bit_offset;
                },
                _ => {}
            }
        }
        Ok(null_bit_map)
    }
}