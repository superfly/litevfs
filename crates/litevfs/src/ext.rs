use crate::LiteVfs;
use log::{info, trace};
use sqlite_vfs::{ffi, RegisterError};

#[no_mangle]
#[cfg(not(feature = "linkable"))]
#[allow(non_snake_case)]
pub extern "C" fn sqlite3_litevfs_init(
    _db: *mut ffi::sqlite3,
    _pzErrMsg: *mut *mut std::ffi::c_char,
    pApi: *mut ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    env_logger::try_init().ok();

    info!("registering LiteVFS");
    let code = match unsafe { sqlite_vfs::DynamicExtension::build(pApi) }.register(
        "litevfs",
        LiteVfs::new("/tmp"),
        true,
    ) {
        Ok(_) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    trace!("register(litevfs) -> {}", code);

    code
}

#[no_mangle]
#[cfg(feature = "linkable")]
pub extern "C" fn sqlite3_litevfs_init(_unused: *const std::ffi::c_char) -> i32 {
    env_logger::try_init().ok();

    info!("registering LiteVFS");
    let code = match sqlite_vfs::LinkedExtension::build().register(
        "litevfs",
        LiteVfs::new("/tmp"),
        true,
    ) {
        Ok(_) => ffi::SQLITE_OK,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    trace!("register(litevfs) -> {}", code);

    code
}
