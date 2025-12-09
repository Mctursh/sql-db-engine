

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

    pub fn decoder_record (columns: Vec<Column>, bytes: &[u8]) -> EncoderResult<Vec<DataTypeValue>> {
        let null_bitmap_size = (columns.len() + BIT_DISCRIMINATOR) / BITS_PER_BYTE;
        let null_bitmap = &bytes[0..null_bitmap_size];
        let mut pointer_index = 0usize;

        let values: Result<Vec<DataTypeValue>, DbError> = columns.iter().enumerate().map(|(index, column)| {
            if is_null(null_bitmap, &index) {
                Ok(DataTypeValue::Null)
                // values.push(&DataTypeValue::Null);
            } else {
                match column.data_type {
                    DataType::Bool => {
                        if bytes[pointer_index] == 1 {
                            // values.push(&DataTypeValue::Bool(true));
                            pointer_index += 1;
                            Ok(DataTypeValue::Bool(true))
                        } else if bytes[pointer_index] == 0 {
                            pointer_index += 1;
                            Ok(DataTypeValue::Bool(false))
                        } else {
                            //TODO use appropriate erro for unrecognized byte in boolean
                            return Err(DbError::UnterminatedString)
                        }
                    },

                    DataType::Int32 => {
                        let i32_bytes = [
                            bytes[pointer_index],
                            bytes[pointer_index + 1],
                            bytes[pointer_index + 2],
                            bytes[pointer_index + 3],
                        ];
                        let i32_val = i32::from_le_bytes(i32_bytes);
                        pointer_index += 4;
                        Ok(DataTypeValue::Int32(i32_val))
                    },
                    DataType::UInt32 => {
                        let i32_bytes = [
                            bytes[pointer_index],
                            bytes[pointer_index + 1],
                            bytes[pointer_index + 2],
                            bytes[pointer_index + 3],
                        ];
                        let i32_val = u32::from_le_bytes(i32_bytes);
                        pointer_index += 4;
                        Ok(DataTypeValue::UInt32(i32_val))
                    },
                    DataType::Int64 => {
                        let i32_bytes = [
                            bytes[pointer_index],
                            bytes[pointer_index + 1],
                            bytes[pointer_index + 2],
                            bytes[pointer_index + 3],
                            bytes[pointer_index + 4],
                            bytes[pointer_index + 5],
                            bytes[pointer_index + 6],
                            bytes[pointer_index + 7],
                        ];
                        let i32_val = i64::from_le_bytes(i32_bytes);
                        pointer_index += 8;
                        Ok(DataTypeValue::Int64(i32_val))
                    },
                    DataType::UInt64 => {
                        let i32_bytes = [
                            bytes[pointer_index],
                            bytes[pointer_index + 1],
                            bytes[pointer_index + 2],
                            bytes[pointer_index + 3],
                            bytes[pointer_index + 4],
                            bytes[pointer_index + 5],
                            bytes[pointer_index + 6],
                            bytes[pointer_index + 7],
                        ];
                        let i32_val = u64::from_le_bytes(i32_bytes);
                        pointer_index += 8;
                        Ok(DataTypeValue::UInt64(i32_val))
                    },
                    DataType::String => {
                        let string_len = u16::from_le_bytes([bytes[pointer_index], bytes[pointer_index + 1]]);
                        let string_byte = &bytes[(pointer_index + 2)..(pointer_index + string_len as usize)];
                        pointer_index += (2 + string_len) as usize;
                        Ok(DataTypeValue::String(String::from_utf8(string_byte.to_vec())?))
                    },
                }
            }
        }).collect();

        Ok(values?)
    }

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
    
    fn is_null (bitmap: &[u8], index: &usize) -> bool {
        let byte_index  = (bitmap.len() + BIT_DISCRIMINATOR) / BITS_PER_BYTE;
        let bit_index = index % BITS_PER_BYTE; // gives use  the remaining bits to the end of the byte
        bitmap[byte_index] >> bit_index & 1 == 1
    }
}