#[allow(clippy::all, non_snake_case, non_camel_case_types, dead_code)]
mod ffi;

use std::slice;
use std::{
    collections::HashMap,
    ffi::{c_char, CString},
    io,
    mem::MaybeUninit,
    ptr,
};

use ffi::emscripten_fetch;

pub fn get(u: url::Url, headers: HashMap<&str, String>) -> Result<Response, Error> {
    let u = CString::new(Into::<String>::into(u))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let u = u.to_bytes_with_nul();

    let mut headers_s: Vec<CString> = Vec::new();
    let mut headers_c: Vec<*const c_char> = Vec::new();
    for (k, v) in headers {
        let k = CString::new(k).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let v = CString::new(v).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        headers_s.push(k);
        let hdr = headers_s.last().unwrap();
        let hdr = hdr.to_bytes_with_nul();
        headers_c.push(hdr.as_ptr() as *const i8);

        headers_s.push(v);
        let hdr = headers_s.last().unwrap();
        let hdr = hdr.to_bytes_with_nul();
        headers_c.push(hdr.as_ptr() as *const i8);
    }
    headers_c.push(ptr::null());

    unsafe {
        let mut attr = MaybeUninit::uninit();
        ffi::emscripten_fetch_attr_init(attr.as_mut_ptr());
        let mut attr = attr.assume_init();

        attr.requestMethod[0] = 'G' as c_char;
        attr.requestMethod[1] = 'E' as c_char;
        attr.requestMethod[2] = 'T' as c_char;
        attr.attributes = ffi::EMSCRIPTEN_FETCH_LOAD_TO_MEMORY
            | ffi::EMSCRIPTEN_FETCH_SYNCHRONOUS
            | ffi::EMSCRIPTEN_FETCH_REPLACE;
        attr.requestHeaders = headers_c.as_ptr();

        let fetch = emscripten_fetch(
            &mut attr as *mut ffi::emscripten_fetch_attr_t,
            u.as_ptr() as *const i8,
        );

        let status = (*fetch).status;
        let resp = Response {
            fetch,
            body: slice::from_raw_parts((*fetch).data as *const u8, (*fetch).numBytes as usize),
        };

        match status {
            200..=299 => Ok(resp),
            status => Err(Error::Status(status, resp)),
        }
    }
}

pub struct Response<'a> {
    fetch: *mut ffi::emscripten_fetch_t,
    pub body: &'a [u8],
}

impl<'a> Drop for Response<'a> {
    fn drop(&mut self) {
        unsafe { ffi::emscripten_fetch_close(self.fetch) };
    }
}

pub enum Error<'a> {
    Status(u16, Response<'a>),
    IO(io::Error),
}

impl<'a> From<io::Error> for Error<'a> {
    fn from(e: io::Error) -> Self {
        Error::IO(e)
    }
}
