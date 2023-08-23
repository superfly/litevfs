use crate::{lfsc, LiteVfs};
use rand::distributions::{Alphanumeric, DistString};
use sqlite_vfs::{ffi, RegisterError};
use std::{env, fs, process};

fn prepare() -> Result<(lfsc::Client, String), Box<dyn std::error::Error + 'static>> {
    let client = lfsc::Client::from_env()?;

    let cache_dir = env::var("LITEVFS_CACHE_DIR").unwrap_or(format!(
        "/tmp/litevfs-{}-{}",
        process::id(),
        Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
    ));
    fs::create_dir_all(&cache_dir)?;

    Ok((client, cache_dir))
}

#[no_mangle]
#[cfg(not(target_os = "emscripten"))]
#[allow(non_snake_case)]
pub extern "C" fn sqlite3_litevfs_init(
    _db: *mut ffi::sqlite3,
    pzErrMsg: *mut *mut std::ffi::c_char,
    pApi: *mut ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    use std::{ffi::CString, ptr};

    env_logger::try_init().ok();

    log::info!("registering LiteVFS");
    let (client, cache_dir) = match prepare() {
        Ok(ret) => ret,
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
        LiteVfs::new(cache_dir, client),
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
#[cfg(target_os = "emscripten")]
pub extern "C" fn sqlite3_wasm_extra_init(_unused: *const std::ffi::c_char) -> std::ffi::c_int {
    env_logger::try_init().ok();

    log::info!("registering LiteVFS");
    let (client, cache_dir) = match prepare() {
        Ok(ret) => ret,
        Err(err) => {
            log::warn!("{}", err);
            return ffi::SQLITE_ERROR;
        }
    };

    let code = match sqlite_vfs::LinkedExtension::build().register(
        "litevfs",
        LiteVfs::new(cache_dir, client),
        true,
    ) {
        Ok(_) => ffi::SQLITE_OK,
        Err(RegisterError::Nul(_)) => ffi::SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    };
    log::debug!("register(litevfs) -> {}", code);

    code
}
