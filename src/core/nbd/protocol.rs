pub const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943;
pub const NBD_IHAVEOPT: u64 = 0x4948_4156_454f_5054;
pub const NBD_REPLY_MAGIC: u32 = 0x6744_6698;
pub const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
pub const NBD_REP_MAGIC: u64 = 0x0003_e889_0455_65a9;

pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;

pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_INFO: u32 = 6;
pub const NBD_OPT_GO: u32 = 7;

pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_INFO: u32 = 3;
pub const NBD_REP_ERR_UNSUP: u32 = 1 << 31 | 1;
pub const NBD_REP_ERR_UNKNOWN: u32 = 1 << 31 | 6;

pub const NBD_INFO_EXPORT: u16 = 0;
pub const NBD_INFO_NAME: u16 = 1;
pub const NBD_INFO_DESCRIPTION: u16 = 2;
pub const NBD_INFO_BLOCK_SIZE: u16 = 3;

pub const NBD_CMD_READ: u16 = 0;
pub const NBD_CMD_WRITE: u16 = 1;
pub const NBD_CMD_DISC: u16 = 2;
pub const NBD_CMD_FLUSH: u16 = 3;

pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;

pub const MIN_BLOCK_SIZE: u32 = 1;
pub const PREFERRED_BLOCK_SIZE: u32 = 4096;
pub const MAX_BLOCK_SIZE: u32 = 32 * 1024 * 1024;
pub const MAX_INFLIGHT_REQUESTS: usize = 128;

pub fn export_flags() -> u16 {
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA
}
