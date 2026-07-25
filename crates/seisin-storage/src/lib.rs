pub mod btree;
pub mod crc;
pub mod datum_log;
pub mod delta;
pub mod node;
pub mod page_store;
pub mod superblock;

pub type PageId = u64;
pub const NULL_PAGE: PageId = 0;
