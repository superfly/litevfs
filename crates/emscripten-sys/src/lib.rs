#![allow(clippy::all, non_snake_case, non_camel_case_types, dead_code)]
/* automatically generated by rust-bindgen 0.66.1 */

pub const EMSCRIPTEN_FETCH_LOAD_TO_MEMORY: u32 = 1;
pub const EMSCRIPTEN_FETCH_STREAM_DATA: u32 = 2;
pub const EMSCRIPTEN_FETCH_PERSIST_FILE: u32 = 4;
pub const EMSCRIPTEN_FETCH_APPEND: u32 = 8;
pub const EMSCRIPTEN_FETCH_REPLACE: u32 = 16;
pub const EMSCRIPTEN_FETCH_NO_DOWNLOAD: u32 = 32;
pub const EMSCRIPTEN_FETCH_SYNCHRONOUS: u32 = 64;
pub const EMSCRIPTEN_FETCH_WAITABLE: u32 = 128;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct emscripten_fetch_attr_t {
    pub requestMethod: [::std::os::raw::c_char; 32usize],
    pub userData: *mut ::std::os::raw::c_void,
    pub onsuccess: ::std::option::Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
    pub onerror: ::std::option::Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
    pub onprogress: ::std::option::Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
    pub onreadystatechange:
        ::std::option::Option<unsafe extern "C" fn(fetch: *mut emscripten_fetch_t)>,
    pub attributes: u32,
    pub timeoutMSecs: u32,
    pub withCredentials: ::std::os::raw::c_int,
    pub destinationPath: *const ::std::os::raw::c_char,
    pub userName: *const ::std::os::raw::c_char,
    pub password: *const ::std::os::raw::c_char,
    pub requestHeaders: *const *const ::std::os::raw::c_char,
    pub overriddenMimeType: *const ::std::os::raw::c_char,
    pub requestData: *const ::std::os::raw::c_char,
    pub requestDataSize: usize,
}
#[test]
fn bindgen_test_layout_emscripten_fetch_attr_t() {
    const UNINIT: ::std::mem::MaybeUninit<emscripten_fetch_attr_t> =
        ::std::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::std::mem::size_of::<emscripten_fetch_attr_t>(),
        144usize,
        concat!("Size of: ", stringify!(emscripten_fetch_attr_t))
    );
    assert_eq!(
        ::std::mem::align_of::<emscripten_fetch_attr_t>(),
        8usize,
        concat!("Alignment of ", stringify!(emscripten_fetch_attr_t))
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).requestMethod) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(requestMethod)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).userData) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(userData)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).onsuccess) as usize - ptr as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(onsuccess)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).onerror) as usize - ptr as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(onerror)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).onprogress) as usize - ptr as usize },
        56usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(onprogress)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).onreadystatechange) as usize - ptr as usize },
        64usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(onreadystatechange)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).attributes) as usize - ptr as usize },
        72usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(attributes)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).timeoutMSecs) as usize - ptr as usize },
        76usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(timeoutMSecs)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).withCredentials) as usize - ptr as usize },
        80usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(withCredentials)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).destinationPath) as usize - ptr as usize },
        88usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(destinationPath)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).userName) as usize - ptr as usize },
        96usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(userName)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).password) as usize - ptr as usize },
        104usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(password)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).requestHeaders) as usize - ptr as usize },
        112usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(requestHeaders)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).overriddenMimeType) as usize - ptr as usize },
        120usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(overriddenMimeType)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).requestData) as usize - ptr as usize },
        128usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(requestData)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).requestDataSize) as usize - ptr as usize },
        136usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_attr_t),
            "::",
            stringify!(requestDataSize)
        )
    );
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct emscripten_fetch_t {
    pub id: u32,
    pub userData: *mut ::std::os::raw::c_void,
    pub url: *const ::std::os::raw::c_char,
    pub data: *const ::std::os::raw::c_char,
    pub numBytes: u64,
    pub dataOffset: u64,
    pub totalBytes: u64,
    pub readyState: ::std::os::raw::c_ushort,
    pub status: ::std::os::raw::c_ushort,
    pub statusText: [::std::os::raw::c_char; 64usize],
    pub __proxyState: u32,
    pub __attributes: emscripten_fetch_attr_t,
}
#[test]
fn bindgen_test_layout_emscripten_fetch_t() {
    const UNINIT: ::std::mem::MaybeUninit<emscripten_fetch_t> = ::std::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::std::mem::size_of::<emscripten_fetch_t>(),
        272usize,
        concat!("Size of: ", stringify!(emscripten_fetch_t))
    );
    assert_eq!(
        ::std::mem::align_of::<emscripten_fetch_t>(),
        8usize,
        concat!("Alignment of ", stringify!(emscripten_fetch_t))
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).id) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(id)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).userData) as usize - ptr as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(userData)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).url) as usize - ptr as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(url)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).data) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(data)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).numBytes) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(numBytes)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).dataOffset) as usize - ptr as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(dataOffset)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).totalBytes) as usize - ptr as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(totalBytes)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).readyState) as usize - ptr as usize },
        56usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(readyState)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).status) as usize - ptr as usize },
        58usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(status)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).statusText) as usize - ptr as usize },
        60usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(statusText)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).__proxyState) as usize - ptr as usize },
        124usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(__proxyState)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).__attributes) as usize - ptr as usize },
        128usize,
        concat!(
            "Offset of field: ",
            stringify!(emscripten_fetch_t),
            "::",
            stringify!(__attributes)
        )
    );
}
extern "C" {
    pub fn emscripten_fetch_attr_init(fetch_attr: *mut emscripten_fetch_attr_t);
}
extern "C" {
    pub fn emscripten_fetch(
        fetch_attr: *mut emscripten_fetch_attr_t,
        url: *const ::std::os::raw::c_char,
    ) -> *mut emscripten_fetch_t;
}
extern "C" {
    pub fn emscripten_fetch_wait(
        fetch: *mut emscripten_fetch_t,
        timeoutMSecs: f64,
    ) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn emscripten_fetch_close(fetch: *mut emscripten_fetch_t) -> ::std::os::raw::c_int;
}
extern "C" {
    pub fn emscripten_fetch_get_response_headers_length(fetch: *mut emscripten_fetch_t) -> usize;
}
extern "C" {
    pub fn emscripten_fetch_get_response_headers(
        fetch: *mut emscripten_fetch_t,
        dst: *mut ::std::os::raw::c_char,
        dstSizeBytes: usize,
    ) -> usize;
}
extern "C" {
    pub fn emscripten_fetch_unpack_response_headers(
        headersString: *const ::std::os::raw::c_char,
    ) -> *mut *mut ::std::os::raw::c_char;
}
extern "C" {
    pub fn emscripten_fetch_free_unpacked_response_headers(
        unpackedHeaders: *mut *mut ::std::os::raw::c_char,
    );
}