#[cfg(not(target_os = "emscripten"))]
pub(crate) use native::{Request, Response};

#[cfg(target_os = "emscripten")]
pub(crate) use emscripten::{Request, Response};

pub(crate) enum Error {
    Status(u16, Box<Response>),
    Transport(String),
}

#[cfg(not(target_os = "emscripten"))]
mod native {
    use serde::{de::DeserializeOwned, Serialize};
    use std::io::Read;
    use url::Url;

    pub(crate) struct Request(ureq::Request);
    pub(crate) struct Response(ureq::Response);

    fn map_err(e: ureq::Error) -> super::Error {
        match e {
            ureq::Error::Status(code, resp) => super::Error::Status(code, Box::new(Response(resp))),
            ureq::Error::Transport(err) => super::Error::Transport(err.to_string()),
        }
    }

    impl Request {
        pub(crate) fn new(method: &str, url: &Url) -> Request {
            Request(ureq::request_url(method, url))
        }

        pub(crate) fn set(self, header: &str, value: &str) -> Self {
            Request(self.0.set(header, value))
        }

        pub(crate) fn call(self) -> Result<Response, super::Error> {
            self.0.call().map(Response).map_err(map_err)
        }

        pub(crate) fn send(self, reader: impl Read) -> Result<Response, super::Error> {
            self.0.send(reader).map(Response).map_err(map_err)
        }

        pub(crate) fn send_json(self, data: impl Serialize) -> Result<Response, super::Error> {
            self.0.send_json(data).map(Response).map_err(map_err)
        }
    }

    impl Response {
        pub(crate) fn header(&self, name: &str) -> Option<&str> {
            self.0.header(name)
        }

        pub(crate) fn into_reader(self) -> Box<dyn Read + 'static> {
            self.0.into_reader()
        }

        pub(crate) fn into_json<T: DeserializeOwned>(self) -> std::io::Result<T> {
            self.0.into_json()
        }
    }
}

#[cfg(target_os = "emscripten")]
mod emscripten {
    use emscripten_sys::{
        emscripten_fetch, emscripten_fetch_attr_init, emscripten_fetch_attr_t,
        emscripten_fetch_close, emscripten_fetch_get_response_headers,
        emscripten_fetch_get_response_headers_length, emscripten_fetch_t,
        EMSCRIPTEN_FETCH_LOAD_TO_MEMORY, EMSCRIPTEN_FETCH_REPLACE, EMSCRIPTEN_FETCH_SYNCHRONOUS,
    };
    use serde::{de::DeserializeOwned, Serialize};
    use std::{
        ffi::{c_char, CString},
        io::{self, Read},
        mem::MaybeUninit,
        ptr, slice,
    };
    use url::Url;

    struct Header(Vec<u8>, usize);

    impl Header {
        fn name(&self) -> &str {
            let bytes = &self.0[0..self.1 - 1];
            std::str::from_utf8(bytes).expect("Legal chars in header name")
        }

        fn value(&self) -> Option<&str> {
            let bytes = &self.0[self.1..];
            std::str::from_utf8(bytes).map(|s| s.trim()).ok()
        }
    }

    pub(crate) struct Request {
        method: String,
        url: CString,
        headers: Vec<Header>,
    }

    pub(crate) struct Response {
        fetch: *mut emscripten_fetch_t,
        body: &'static [u8],
        headers: Vec<Header>,
    }

    impl Request {
        pub(crate) fn new(method: &str, url: &Url) -> Request {
            Request {
                method: method.into(),
                url: CString::new(url.as_str()).unwrap(),
                headers: Vec::new(),
            }
        }

        pub(crate) fn set(mut self, header: &str, value: &str) -> Self {
            let combined = format!("{}\0{}\0", header, value);
            self.headers.retain(|h| h.name() != header);
            self.headers.push(Header(combined.into(), header.len() + 1));
            self
        }

        pub(crate) fn call(self) -> Result<Response, super::Error> {
            let headers = self.headers();
            let mut req = self.fetch_attr(&headers);

            let resp = unsafe {
                emscripten_fetch(&mut req as *mut emscripten_fetch_attr_t, self.url.as_ptr())
            };

            self.response(resp)
        }

        pub(crate) fn send(self, mut reader: impl Read) -> Result<Response, super::Error> {
            let mut body = Vec::new();
            reader
                .read_to_end(&mut body)
                .map_err(|e| super::Error::Transport(e.to_string()))?;

            self.do_send(&body)
        }

        pub(crate) fn send_json(mut self, data: impl Serialize) -> Result<Response, super::Error> {
            if self.header("Content-Type").is_none() {
                self = self.set("Content-Type", "application/json");
            }

            let json_bytes = serde_json::to_vec(&data)
                .expect("Failed to serialize data passed to send_json into JSON");

            self.do_send(&json_bytes)
        }

        pub(crate) fn header(&self, header: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|h| h.name().eq_ignore_ascii_case(header))
                .and_then(|h| h.value())
        }

        fn do_send(self, body: &[u8]) -> Result<Response, super::Error> {
            let headers = self.headers();
            let mut req = self.fetch_attr(&headers);
            req.requestData = body.as_ptr() as *const i8;
            req.requestDataSize = body.len();

            let resp = unsafe {
                emscripten_fetch(&mut req as *mut emscripten_fetch_attr_t, self.url.as_ptr())
            };

            self.response(resp)
        }

        fn headers(&self) -> Vec<*const c_char> {
            let mut headers = Vec::with_capacity(self.headers.len() * 2 + 1);
            for h in &self.headers {
                headers.push(h.0.as_ptr() as *const i8);
                unsafe { headers.push(h.0.as_ptr().add(h.1) as *const i8) };
            }
            headers.push(ptr::null());

            headers
        }

        fn fetch_attr(&self, headers: &[*const c_char]) -> emscripten_fetch_attr_t {
            let mut attr = unsafe {
                let mut attr = MaybeUninit::uninit();
                emscripten_fetch_attr_init(attr.as_mut_ptr());
                let mut attr = attr.assume_init();
                attr.requestMethod[..self.method.len()]
                    .copy_from_slice(&*(self.method.as_bytes() as *const _ as *const [i8]));

                attr
            };

            attr.attributes = EMSCRIPTEN_FETCH_LOAD_TO_MEMORY
                | EMSCRIPTEN_FETCH_SYNCHRONOUS
                | EMSCRIPTEN_FETCH_REPLACE;
            attr.requestHeaders = headers.as_ptr();

            attr
        }

        fn response(&self, resp: *mut emscripten_fetch_t) -> Result<Response, super::Error> {
            let headers = unsafe {
                let len = emscripten_fetch_get_response_headers_length(resp) + 1;
                let mut headers = Vec::<u8>::with_capacity(len);
                emscripten_fetch_get_response_headers(resp, headers.as_mut_ptr() as *mut i8, len);
                headers.set_len(len);

                headers
                    .split(|b| b == &(b'\n'))
                    .filter_map(|line| {
                        let idx = line.iter().position(|b| b == &(b':'))?;

                        let mut header = Vec::with_capacity(line.len() + 1);
                        header.extend_from_slice(&line[0..idx]);
                        header.push(0);
                        header.extend_from_slice(&line[idx + 1..]);
                        header.push(0);

                        Some(Header(header, idx + 1))
                    })
                    .collect()
            };

            let (status, resp) = unsafe {
                let status = (*resp).status;
                let resp = Response {
                    fetch: resp,
                    body: slice::from_raw_parts(
                        (*resp).data as *const u8,
                        (*resp).numBytes as usize,
                    ),
                    headers,
                };

                (status, resp)
            };

            match status {
                200..=299 => Ok(resp),
                status => Err(super::Error::Status(status, Box::new(resp))),
            }
        }
    }

    impl Response {
        pub(crate) fn header(&self, header: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|h| h.name().eq_ignore_ascii_case(header))
                .and_then(|h| h.value())
        }

        pub(crate) fn into_reader(self) -> Box<dyn Read + 'static> {
            Box::new(self)
        }

        pub(crate) fn into_json<T: DeserializeOwned>(self) -> io::Result<T> {
            let reader = self.into_reader();
            serde_json::from_reader(reader).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to read JSON: {}", e),
                )
            })
        }
    }

    impl Read for Response {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.body.read(buf)
        }
    }

    impl Drop for Response {
        fn drop(&mut self) {
            unsafe {
                emscripten_fetch_close(self.fetch);
            }
        }
    }
}
