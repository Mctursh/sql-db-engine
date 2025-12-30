
pub mod Encoder {
    use std::iter::zip;

    use crate::constants::{BIT_DISCRIMINATOR, BITS_PER_BYTE, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_BYTE_SIZE, STRING_LENGTH_SIZE};
    use crate::error::DbError;
    use crate::types::{Column, DataType, DataTypeValue, PageHeader, Slot};

    type EncoderResult<T> = Result<T, DbError>;

    // pub fn encode_record (columns: &Vec<DataType>, values: &Vec<DataTypeValue>) -> EncoderResult<Vec<u8>> {
    pub fn encode_record (columns: &Vec<Column>, values: &Vec<DataTypeValue>) -> EncoderResult<Vec<u8>> {
        if columns.len() != values.len() {
            // TODO; use appropriate error
            return Err(DbError::UnterminatedString)
        }

        //TODO: add validation for encoding
        let mut encoded_bytes = get_bitmap(&values)?;

        for (column, value) in zip(columns, values) {
        // for index in 0..values.len() {

            //check if not nullable value receives null
            if let DataTypeValue::Null = value {
                if !column.nullable {
                    // Todo: Use prooper Error for non-null violation
                    return Err(DbError::UnterminatedString)
                }
            }

            match &value {
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
                    //check string length constraint
                    if let Some(len) = column.max_length {
                        if v.len() > (len as usize) {
                            // Todo: Use proper Error for string overflow error
                            return Err(DbError::UnterminatedString)
                        }
                    }

                    let string_length_byte: [u8; STRING_LENGTH_SIZE] = (v.len() as u16).to_le_bytes();
                    let string_len = v.len();
                    println!("string length is {string_len}");
                    println!("string is {v}");
                    println!("string length byte is {string_length_byte:?}");
                    let str_bytes = v.as_bytes();
                    encoded_bytes.extend_from_slice(&string_length_byte);
                    encoded_bytes.extend_from_slice(&str_bytes);
                    println!("string byte is {str_bytes:?}");
                },
                _ => {}
            }
        }
        
        Ok(encoded_bytes)
    }

    // pub fn decode_record (columns: Vec<DataType>, bytes: &[u8]) -> EncoderResult<Vec<DataTypeValue>> {
    pub fn decode_record (columns: Vec<Column>, bytes: &[u8]) -> EncoderResult<Vec<DataTypeValue>> {
        let null_bitmap_size = (columns.len() + BIT_DISCRIMINATOR) / BITS_PER_BYTE;
        let null_bitmap = &bytes[0..null_bitmap_size];
        let mut pointer_index = 0 + null_bitmap_size;

        println!("The bytes are {bytes:?}");

        let values: Result<Vec<DataTypeValue>, DbError> = columns.iter().enumerate().map(|(index, column)| {
            if is_null(null_bitmap, &index) {
                Ok(DataTypeValue::Null)
                // values.push(&DataTypeValue::Null);
            } else {
                match column.data_type {
                // match column.data_type {
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
                        let str_len = [bytes[pointer_index + 1], bytes[pointer_index + 2]];
                        // let str_len = [bytes[pointer_index], bytes[pointer_index + 1]];
                        println!("str_len length is {str_len:?}");
                        println!("Pointer index is {pointer_index:?}");


                        let string_len = u16::from_le_bytes([bytes[pointer_index], bytes[pointer_index + 1]]);
                        let end_index = pointer_index + string_len as usize;
                        let byte_len = bytes.len();
                        println!("Bytes length is {byte_len}");
                        println!("String length is {string_len}");
                        println!("End index length is {end_index}");
                        let string_byte = &bytes[(pointer_index + STRING_LENGTH_SIZE)..(pointer_index + STRING_LENGTH_SIZE + string_len as usize)];
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

        println!("bitmap size {bitmap_size:?}");
        println!("Generated bitmap {null_bit_map:?}");

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
        let byte_index  = ((bitmap.len() + BIT_DISCRIMINATOR) / BITS_PER_BYTE) - 1;
        let bit_index = index % BITS_PER_BYTE; // gives use  the remaining bits to the end of the byte

        println!("Byte Index {byte_index:?}");
        println!("Bitmap {bitmap:?}");
        println!("Bit index {bit_index:?}");
        // bitmap[byte_index - 1] >> bit_index & 1 == 1
        bitmap[byte_index] >> bit_index & 1 == 1
    }

    fn get_page_free_slot_offset (page_bytes: &[u8; PAGE_SIZE as usize], record_count: u32) -> u32 {
        PAGE_SIZE - ((record_count + 1) * SLOT_BYTE_SIZE)
    }

    pub fn get_slot (page_bytes: &[u8; PAGE_SIZE as usize], slot_index: &u32) -> EncoderResult<Slot> {
        let slot_offset = (PAGE_SIZE - ((slot_index + 1) * SLOT_BYTE_SIZE));
        // let slot_offset = (PAGE_SIZE * ((slot_index + 1) * SLOT_BYTE_SIZE));
        // TODO: validtaion needed here that page data doesn't get to the offset 
        let slot_bytes: [u8; SLOT_BYTE_SIZE as usize] = [
            page_bytes[slot_offset as usize],
            page_bytes[(slot_offset + 1) as usize],
            page_bytes[(slot_offset + 2) as usize],
            page_bytes[(slot_offset + 3) as usize],
        ];
        Ok(Slot::from_bytes(slot_bytes))
    }
    
    pub fn read_slot (page_bytes: &[u8; PAGE_SIZE as usize], slot: Slot) -> EncoderResult<&[u8]> {
        let Slot { length, offset } = slot;
        // for i in 0..length {
        //     slot_data.push(page_bytes[(offset + i) as usize]);
        // }

        Ok(&page_bytes[(offset as usize)..((offset + length) as usize)])
        
        // Ok(slot_data)

    }

    pub fn set_slot (page_bytes: &mut [u8; PAGE_SIZE as usize], slot_index: u32, offset: u16, length: u16) -> EncoderResult<&mut [u8; PAGE_SIZE as usize]> {
        let slot_offset = (PAGE_SIZE * ((slot_index + 1) * SLOT_BYTE_SIZE)) as usize;
        // TODO: validtaion needed here that page data doesn't get to the offset 
        let slot_bytes = Slot::to_bytes(length, offset);
        page_bytes[slot_offset] = slot_bytes[0];
        page_bytes[slot_offset + 1] = slot_bytes[1];
        page_bytes[slot_offset + 2] = slot_bytes[2];
        page_bytes[slot_offset + 3] = slot_bytes[3];

        Ok(page_bytes)
    }

    pub fn calculate_free_space (page_bytes: &[u8; PAGE_SIZE as usize]) -> EncoderResult<u32> {
        let page_header = PageHeader::from_bytes(&page_bytes);
        let free_space_offset = page_header.free_space_offset as u32;
        let record_count = page_header.record_count as u32;
        let slot_directory_start = PAGE_SIZE - record_count * SLOT_BYTE_SIZE;
        Ok(slot_directory_start - free_space_offset)
    }

    pub fn can_fit_record (page_bytes: &[u8; PAGE_SIZE as usize], record_size: usize) -> EncoderResult<bool> {
        Ok((calculate_free_space(page_bytes)? as usize) > record_size)
        // Ok(false)
    }

    pub fn insert_record (page_bytes: &mut [u8; PAGE_SIZE as usize], record_bytes: &[u8]) -> EncoderResult<(u32)> {
        let mut page_header = PageHeader::from_bytes(page_bytes);
        let free_space_offset = page_header.free_space_offset as usize;
        let record_count = page_header.record_count as u32;

        let space_needed_for_entry = record_bytes.len() + (SLOT_BYTE_SIZE as usize);

        if !can_fit_record(&page_bytes, space_needed_for_entry)? {
            // TODO use appropriate error eg PageFull error
            return Err(DbError::UnterminatedString)
        }

        let new_slot_offset = get_page_free_slot_offset(&page_bytes, record_count) as usize;

        let slot_bytes = Slot::to_bytes((record_bytes.len() as u16), (free_space_offset as u16));

        // write record bytes
        page_bytes[free_space_offset..(free_space_offset + record_bytes.len())]
        .copy_from_slice(&record_bytes);
    
        // write slot bytes
        page_bytes[new_slot_offset..(new_slot_offset + (SLOT_BYTE_SIZE as usize))]
            .copy_from_slice(&slot_bytes);

        page_header.record_count += 1;
        page_header.free_space_offset = (free_space_offset + record_bytes.len()) as u16;

        page_bytes[0..PAGE_HEADER_SIZE].copy_from_slice(&page_header.to_header_bytes());

        //new slot index is the old record count.
        Ok(record_count)
    }

    pub fn read_record (page_bytes: &[u8; PAGE_SIZE as usize], slot_index: u32) -> EncoderResult<&[u8]> {
        let page_header = PageHeader::from_bytes(page_bytes);
        // let free_space_offset = page_header.free_space_offset as usize;
        let record_count = page_header.record_count as u32;

        if slot_index >= record_count {
            // Todo: need to use appropriate overflow error
            return Err(DbError::UnterminatedString)
        }
        
        let slot = get_slot(&page_bytes, &slot_index)?;

        println!("slot data is {slot:?}");
        
        if slot.offset == 0 { // means record deleted
            // Todo use appropraite error for deleted record.
            println!("Failed here");
            return Err(DbError::UnterminatedString)
        }

        Ok(&read_slot(&page_bytes, slot)?)

        // Ok(())
    }
    
    pub fn delete_record (page_bytes: &mut [u8; PAGE_SIZE as usize], slot_index: u32) -> EncoderResult<()> {
        let mut page_header = PageHeader::from_bytes(page_bytes);
        let mut slot = get_slot(page_bytes, &slot_index)?;
        if slot.offset == 0 { // deleted slot (Tombstone)
            //Todo: hamdle proper error variant for deleting an already deleted record.
            return Err(DbError::UnterminatedString)
        }
        slot.offset = 0;
        let new_slot_offset = get_page_free_slot_offset(&page_bytes, slot_index) as usize;
        // let new_slot_offset = get_page_free_slot_offset(&page_bytes, page_header.record_count as u32) as usize;
        let slot_bytes = Slot::to_bytes(slot.length, slot.offset);

         // write slot bytes
         page_bytes[new_slot_offset..(new_slot_offset + (SLOT_BYTE_SIZE as usize))]
            .copy_from_slice(&slot_bytes);

        Ok(())
    }

    pub fn iterate_records(page_bytes: &[u8; PAGE_SIZE as usize]) {}
}