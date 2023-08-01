use crate::{lfsc, LiteVfs};
use sqlite_vfs::{ffi, RegisterError};

#[no_mangle]
#[cfg(not(feature = "linkable"))]
#[allow(non_snake_case)]
pub extern "C" fn sqlite3_litevfs_init(
    _db: *mut ffi::sqlite3,
    pzErrMsg: *mut *mut std::ffi::c_char,
    pApi: *mut ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    use std::{ffi::CString, ptr};

    env_logger::try_init().ok();

    log::info!("registering LiteVFS");
    let client = match lfsc::Client::from_env() {
        Ok(client) => client,
        Err(err) if !pzErrMsg.is_null() => {
            let msg = CString::new(err.to_string()).unwrap();
            let msg_slice = msg.to_bytes_with_nul();
            unsafe {
                *pzErrMsg = (*pApi).malloc64.unwrap()(msg_slice.len() as u64) as *mut i8;
                ptr::copy(msg_slice.as_ptr() as *const i8, *pzErrMsg, msg_slice.len());
            };
            return ffi::SQLITE_ERROR;
        }
        Err(err) => {
            log::warn!("{}", err);
            return ffi::SQLITE_ERROR;
        }
    };

    let code = match unsafe { sqlite_vfs::DynamicExtension::build(pApi) }.register(
        "litevfs",
        LiteVfs::new("/tmp", client),
        true,
    ) {
        Ok(_) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    log::debug!("register(litevfs) -> {}", code);

    code
}

#[no_mangle]
#[cfg(feature = "linkable")]
pub extern "C" fn sqlite3_litevfs_init(_unused: *const std::ffi::c_char) -> i32 {
    env_logger::try_init().ok();

    log::info!("registering LiteVFS");
    let client = match lfsc::Client::from_env() {
        Ok(client) => client,
        Err(err) => {
            log::warn!("{}", err);
            return ffi::SQLITE_ERROR;
        }
    };

    let code = match sqlite_vfs::LinkedExtension::build().register(
        "litevfs",
        LiteVfs::new("/tmp", client),
        true,
    ) {
        Ok(_) => ffi::SQLITE_OK,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    log::debug!("register(litevfs) -> {}", code);

    code
}
