// use std::error::Error;

use std::fs::File;

use rust_api::write_page;

use crate::constants::{
    PAGE_SIZE,
    DEFAULT_BUFFER_POOL_SIZE
};

use crate::error::DbError;

type PoolResult<T> = Result<T, DbError>;
type PoolPageReuslt<T> = PoolResult<Option<T>>;

#[derive(Debug, Copy, Clone)]
pub struct PoolEntry {
    page_id: Option<u32>,
    data: [u8; PAGE_SIZE as usize],
    is_dirty: bool,
    pin_count: u32,
    last_accessed: u64
}

impl Default for PoolEntry {
    fn default() -> Self {
        Self {
            page_id: None,
            data: [0u8; PAGE_SIZE as usize],
            is_dirty: false,
            pin_count: 0,
            last_accessed: 0
        }
    }
}

impl PoolEntry {
    pub fn cleanup (&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        } 
    }
}

pub struct BufferPool {
    entries: [PoolEntry; DEFAULT_BUFFER_POOL_SIZE],
    access_counter: u64, // increments on each access, used instead of timestamps.
}

impl BufferPool {
    pub fn new () -> PoolResult<BufferPool> {
        Ok(Self {
            entries: [Default::default(); DEFAULT_BUFFER_POOL_SIZE],
            access_counter: 0
        })
    }

    pub fn find_page (&mut self, page_id: u32) -> PoolPageReuslt<&mut PoolEntry> {
       let page =  self.entries.iter_mut().find(|entry| {
            if let Some(page_number) = entry.page_id {
                page_number == page_id
            } else {
                false
            }
        });
        //TODO: add pin logic to this function and impl an unpin upon drop
        if let Some(actual_page) = page {
            actual_page.pin_count += 1;
            Ok(Some(actual_page))
        } else {
            Ok(None)
        }

    }

    pub fn find_empty_slot (self: &mut BufferPool) -> PoolResult<(&mut PoolEntry)> {
        let mut empty_slot: Option<usize> = None;
        // let mut empty_slot: Option<&mut PoolEntry> = None;
        for (index, entry) in self.entries.iter().enumerate()    
        {
            if let None = entry.page_id {
                empty_slot = Some(index);
                break
            } else {
                continue;
            }
        };

        if let Some(slot) = empty_slot {
            //at this point we are sure we found a free page in the bufferPool
            self.entries[slot].pin_count += 1; // increase pin count
            Ok(&mut self.entries[slot])
        } else {
            // this mean we didn't find find an empty Slot in the BufferPool then we need to generate
            self.evict_lru()
        }
    }

    pub fn evict_lru (&mut self) -> PoolResult<&mut PoolEntry> {
        let mut least_last_accessed_value = 0_u64;
        let mut evicted_pool: Option<&mut PoolEntry> = None;
        for entry in self.entries.iter_mut()
            .filter(|entry | entry.pin_count == 0) // returns nly pages with Zero pin count i.e being used in the code currently.
        {
            if entry.last_accessed <= least_last_accessed_value {
                least_last_accessed_value = entry.last_accessed;
                evicted_pool = Some(entry)
            }
        };
        if let Some(pool) = evicted_pool {
            pool.pin_count += 1; // increase pin count
            return Ok(pool);
        } else {
            //TODO: sample error, should fix to appropriate error, All pages are currenlt in use will need to expand bufferPool
            return Err(DbError::PageFull { page_id: 2 })
        }
    }

    pub fn flush_page (&mut self, page_id: u32, file: &File) -> PoolResult<()> {
        let page = self.find_page(page_id)?;
        if let Some(page) = page {
            write_page(file, page_id, &page.data)?;
            Ok(())
        } else {
            //TODO: will use the appropriate error for flush failure
            Err(DbError::UnterminatedString)
        }
    }
}
mod pool {
    use crate::error::DbError;

    // pub fn flush_page (page_id: u32, ) -> Result<(), DbError> {

    //     Ok(())
    // }
    // pub fn new (size: [PoolEntry; DEFAULT_BUFFER_POOL_SIZE]) -> Result<()> {
    //     Ok(())
    // }
    // pub fn get_page () {}
    // pub fn pin_page () {}
    // pub fn unpin_page () {}
    // pub fn flush_all () {}
}