use crate::LiteVfs;
use log::{info, trace};
use sqlite_vfs::{register, RegisterError};
use std::ffi::{c_char, c_int};

const SQLITE_OK_LOAD_PERMANENTLY: i32 = 256;
const SQLITE_ERROR: i32 = 1;

#[no_mangle]
pub extern "C" fn sqlite3_litevfs_init(_dummy: *const c_char) -> c_int {
    env_logger::init();

    info!("registering LiteVFS");
    let code = match register("litevfs", LiteVfs::new("/tmp"), true) {
        Ok(_) => SQLITE_OK_LOAD_PERMANENTLY,
        Err(RegisterError::Nul(_)) => SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    trace!("register(litevfs) -> {}", code);

    code
}
