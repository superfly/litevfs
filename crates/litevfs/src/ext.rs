use crate::LiteVfs;
use log::{info, trace};
use sqlite_vfs::{ffi, register, RegisterError};

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn sqlite3_litevfs_init(
    _db: *mut ffi::sqlite3,
    _pzErrMsg: *mut *mut std::ffi::c_char,
    pApi: *mut ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    env_logger::init();

    sqlite_vfs::init_extention(pApi);

    info!("registering LiteVFS");
    let code = match register("litevfs", LiteVfs::new("/tmp"), true) {
        Ok(_) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    trace!("register(litevfs) -> {}", code);

    code
}
