#[allow(clippy::all, non_snake_case, non_camel_case_types, dead_code)]
mod ffi;

use std::ffi::CStr;
use std::slice;
use std::{
    collections::HashMap,
    ffi::{c_char, CString},
    io,
    mem::MaybeUninit,
    ptr,
};

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

        let fetch = ffi::emscripten_fetch(
            &mut attr as *mut ffi::emscripten_fetch_attr_t,
            u.as_ptr() as *const i8,
        );

        let headers_length = ffi::emscripten_fetch_get_response_headers_length(fetch);
        let mut headers_s: Vec<std::ffi::c_char> = vec![0; headers_length + 1];
        let headers_c = headers_s.as_mut_ptr();
        ffi::emscripten_fetch_get_response_headers(fetch, headers_c, headers_s.len());
        let headers_u = ffi::emscripten_fetch_unpack_response_headers(headers_c);
        let mut headers_u_copy = headers_u;

        let mut headers = HashMap::new();
        while (*headers_u_copy).is_null() {
            let k = *headers_u_copy;
            headers_u_copy = headers_u_copy.add(1);
            let v = *headers_u_copy;
            headers_u_copy = headers_u_copy.add(1);

            let k = CStr::from_ptr(k);
            let v = CStr::from_ptr(v);

            headers.insert(
                k.to_string_lossy().into_owned().to_lowercase(),
                v.to_string_lossy().trim().to_owned(),
            );
        }
        ffi::emscripten_fetch_free_unpacked_response_headers(headers_u);

        let status = (*fetch).status;
        let resp = Response {
            fetch,
            body: slice::from_raw_parts((*fetch).data as *const u8, (*fetch).numBytes as usize),
            headers,
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
    pub headers: HashMap<String, String>,
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
