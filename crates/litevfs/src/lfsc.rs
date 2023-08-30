use std::{collections::HashMap, env, fmt, io, sync};

use crate::PosLogger;

/// All possible errors returned by the LFSC client.
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("transport level: {0}")]
    Transport(String),
    #[error("ltx position mismatch: {0}")]
    PosMismatch(ltx::Pos),
    #[error("LFSC: {0}")]
    Lfsc(LfscError),
    #[error("body: {0}")]
    Body(#[from] io::Error),
    #[error("environment: {0}")]
    Env(String),
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Transport(e) => io::Error::new(io::ErrorKind::Other, e),
            Error::PosMismatch(_) => io::Error::new(io::ErrorKind::InvalidData, e),
            Error::Lfsc(e) if e.http_code == 404 => io::Error::new(io::ErrorKind::NotFound, e),
            Error::Lfsc(e) if e.http_code == 409 => io::Error::new(io::ErrorKind::AlreadyExists, e),
            Error::Lfsc(e) => io::Error::new(io::ErrorKind::Other, e),
            Error::Body(e) => e,
            Error::Env(s) => io::Error::new(io::ErrorKind::Other, s),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Body(io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub(crate) struct LfscError {
    pub(crate) http_code: u16,
    pub(crate) code: String,
    pub(crate) error: String,
}

impl fmt::Display for LfscError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        write!(
            f,
            "{} ({}): {}",
            match self.http_code {
                400 => "validation",
                401 => "auth",
                404 => "notfound",
                409 => "conflict",
                422 => "unprocessable",
                _ => "unknown",
            },
            self.code,
            self.error
        )
    }
}

#[derive(Debug, serde::Deserialize)]
struct LfscErrorRepr {
    code: String,
    error: String,
    pos: Option<ltx::Pos>,
}

/// A LiteFS Cloud client.
pub(crate) struct Client {
    host: url::Url,
    token: Option<String>,
    cluster: Option<String>,
    cluster_id: Option<String>,
    instance_id: sync::RwLock<Option<String>>,
}

/// A single database page fetched from LFSC.
#[serde_with::serde_as]
#[derive(Debug, PartialEq, serde::Deserialize)]
pub(crate) struct Page {
    #[serde_as(as = "serde_with::base64::Base64")]
    data: Vec<u8>,
    #[serde(rename = "pgno")]
    number: ltx::PageNum,
}

impl Page {
    /// Get the page number.
    pub(crate) fn number(&self) -> ltx::PageNum {
        self.number
    }

    /// Consume the page and return the underlying buffer.
    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.data
    }
}

impl AsRef<[u8]> for Page {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// A set of pages changed since previously known state.
#[derive(Debug)]
pub(crate) enum Changes {
    All(ltx::Pos),
    Pages(ltx::Pos, Option<Vec<ltx::PageNum>>),
}

#[derive(Debug)]
pub(crate) enum LeaseOp<'a> {
    Acquire(std::time::Duration),
    Refresh(&'a str, std::time::Duration),
}

impl<'a> fmt::Display for LeaseOp<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        match self {
            LeaseOp::Acquire(dur) => write!(f, "acquire({}ms)", dur.as_millis()),
            LeaseOp::Refresh(id, dur) => write!(f, "refresh({}, {}ms", id, dur.as_millis()),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
pub(crate) struct Lease {
    pub(crate) id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) expires_at: time::OffsetDateTime,
}

impl fmt::Display for Lease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        write!(f, "{}/{}", self.id, self.expires_at)
    }
}

impl Client {
    const CLUSTER_ID_LEN: usize = 20;
    const CLUSTER_ID_PREFIX: &'static str = "LFSC";

    pub(crate) fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub(crate) fn from_env() -> Result<Client> {
        let builder = Client::builder().token(
            &env::var("LITEFS_CLOUD_TOKEN")
                .map_err(|_| Error::Env("LITEFS_CLOUD_TOKEN env var is not set".into()))?,
        );
        let builder = match env::var("LITEFS_CLOUD_CLUSTER") {
            Ok(cluster) => builder.cluster(&cluster),
            Err(_) => builder,
        };
        let builder = match env::var("LITEFS_CLOUD_HOST") {
            Ok(host) => builder.host(
                &host
                    .parse()
                    .map_err(|e: url::ParseError| Error::Env(e.to_string()))?,
            ),
            Err(_) => builder,
        };

        let mut client = builder.build();

        let info = client.info()?;
        client.set_cluster_id(if let Some(cluster_id) = info.cluster_id {
            cluster_id
        } else {
            Client::generate_cluster_id()
        });

        log::info!(
            "[lfsc] from_env: host = {}, cluster = {:?}, cluster_id = {:?}",
            client.host,
            client.cluster,
            client.cluster_id
        );

        Ok(client)
    }

    pub(crate) fn set_cluster_id(&mut self, id: String) {
        self.cluster_id = Some(id)
    }

    pub(crate) fn generate_cluster_id() -> String {
        use rand::Rng;

        let mut buf = [0; (Client::CLUSTER_ID_LEN - Client::CLUSTER_ID_PREFIX.len()) / 2];
        rand::thread_rng().fill(&mut buf);

        format!("{}{}", Client::CLUSTER_ID_PREFIX, hex::encode_upper(buf))
    }

    pub(crate) fn pos_map(&self) -> Result<HashMap<String, ltx::Pos>> {
        log::debug!("[lfsc] pos_map");

        let mut u = self.host.clone();
        u.set_path("/pos");

        self.call("GET", u)
    }

    #[cfg(not(target_os = "emscripten"))]
    pub(crate) fn write_tx(
        &self,
        db: &str,
        ltx: impl io::Read,
        ltx_len: u64,
        lease: &str,
    ) -> Result<()> {
        log::debug!("[lfsc] write_tx: db = {}", db);

        let mut u = self.host.clone();
        u.set_path("/db/tx");
        u.query_pairs_mut().append_pair("db", db);

        let req = self
            .make_request("POST", u)
            .set("Content-Length", &ltx_len.to_string())
            .set("Lfsc-Lease-Id", lease);
        let resp = self.process_response(req.send(ltx))?;

        // consume the body (and ignore any errors) to reuse the connection
        io::copy(&mut resp.into_reader(), &mut io::sink()).ok();

        Ok(())
    }

    #[cfg(target_os = "emscripten")]
    pub(crate) fn write_tx(&self, _db: &str, _ltx: impl io::Read, _ltx_len: u64) -> Result<()> {
        return Err(io::Error::new(io::ErrorKind::Other, "not implemented").into());
    }

    pub(crate) fn get_page(
        &self,
        db: &str,
        pos: ltx::Pos,
        pgno: ltx::PageNum,
    ) -> Result<Vec<Page>> {
        log::debug!(
            "[lfsc] get_page: db = {}, pos = {}, pgno = {}",
            db,
            pos,
            pgno
        );

        #[derive(serde::Deserialize)]
        struct GetPageResponse {
            pages: Vec<Page>,
        }

        let mut u = self.host.clone();
        u.set_path("/db/page");
        u.query_pairs_mut()
            .append_pair("db", db)
            .append_pair("pos", &pos.to_string())
            .append_pair("pgno", &pgno.to_string());

        Ok(self.call::<GetPageResponse>("GET", u)?.pages)
    }

    pub(crate) fn info(&self) -> Result<Info> {
        log::debug!("[lfsc] info");

        let mut u = self.host.clone();
        u.set_path("/info");

        self.call("GET", u)
    }

    pub(crate) fn sync(&self, db: &str, pos: Option<ltx::Pos>) -> Result<Changes> {
        log::debug!("[lfsc] sync: db = {}, pos = {}", db, PosLogger(&pos));

        let mut u = self.host.clone();
        u.set_path("/db/sync");
        u.query_pairs_mut().append_pair("db", db);
        if let Some(pos) = pos {
            u.query_pairs_mut().append_pair("pos", &pos.to_string());
        }

        #[derive(serde::Deserialize)]
        struct SyncResponse {
            pos: ltx::Pos,
            pgnos: Option<Vec<ltx::PageNum>>,
            all: Option<bool>,
        }

        let resp = self.call::<SyncResponse>("GET", u)?;

        match resp.all {
            Some(true) => Ok(Changes::All(resp.pos)),
            _ => Ok(Changes::Pages(resp.pos, resp.pgnos)),
        }
    }

    pub(crate) fn acquire_lease(&self, db: &str, op: LeaseOp) -> Result<Lease> {
        log::debug!("[lfscs] acquire_lease: db = {}, op = {}", db, op);

        let mut u = self.host.clone();
        u.set_path("/lease");
        u.query_pairs_mut().append_pair("db", db);
        match op {
            LeaseOp::Acquire(duration) => u
                .query_pairs_mut()
                .append_pair("duration", &duration.as_millis().to_string()),

            LeaseOp::Refresh(lease, duration) => u
                .query_pairs_mut()
                .append_pair("id", lease)
                .append_pair("duration", &duration.as_millis().to_string()),
        };

        self.call::<Lease>("POST", u)
    }

    pub(crate) fn release_lease(&self, db: &str, lease: Lease) -> Result<()> {
        log::debug!("[lfscs] release_lease: db = {}, lease = {}", db, lease.id);

        let mut u = self.host.clone();
        u.set_path("/lease");
        u.query_pairs_mut()
            .append_pair("db", db)
            .append_pair("id", &lease.id);

        let req = self.make_request("DELETE", u);
        let resp = self.process_response(req.call())?;
        // consume the body (and ignore any errors) to reuse the connection
        io::copy(&mut resp.into_reader(), &mut io::sink()).ok();

        Ok(())
    }

    #[cfg(not(target_os = "emscripten"))]
    fn call<R>(&self, method: &str, u: url::Url) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let req = self.make_request(method, u);
        let resp = self.process_response(req.call())?;

        Ok(resp.into_json()?)
    }

    #[cfg(not(target_os = "emscripten"))]
    fn make_request(&self, method: &str, mut u: url::Url) -> ureq::Request {
        if let Some(ref cluster) = self.cluster {
            u.query_pairs_mut().append_pair("cluster", cluster);
        }

        let mut req = ureq::request_url(method, &u);
        if let Some(ref token) = self.token {
            req = req.set("Authorization", token);
        }
        if let Some(instance_id) = self.instance_id.read().unwrap().as_deref() {
            req = req.set("fly-force-instance-id", instance_id);
        }
        if let Some(ref cluster_id) = self.cluster_id {
            req = req.set("Litefs-Cluster-Id", cluster_id)
        }

        req
    }

    #[cfg(not(target_os = "emscripten"))]
    fn process_response(
        &self,
        resp: std::result::Result<ureq::Response, ureq::Error>,
    ) -> Result<ureq::Response> {
        match resp {
            Ok(resp) => {
                let mut instance_id = self.instance_id.write().unwrap();
                if instance_id.as_deref() != resp.header("Lfsc-Instance-Id") {
                    *instance_id = resp.header("Lfsc-Instance-Id").map(Into::into);
                }

                Ok(resp)
            }
            Err(ureq::Error::Transport(err)) => Err(Error::Transport(err.to_string())),
            Err(ureq::Error::Status(code, body)) => {
                let repr: LfscErrorRepr = body.into_json()?;
                match repr.pos {
                    Some(pos) if repr.code == "EPOSMISMATCH" => Err(Error::PosMismatch(pos)),
                    _ => Err(Error::Lfsc(LfscError {
                        http_code: code,
                        code: repr.code,
                        error: repr.error,
                    })),
                }
            }
        }
    }

    #[cfg(target_os = "emscripten")]
    fn call<R>(&self, method: &str, mut u: url::Url) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        if let Some(ref cluster) = self.cluster {
            u.query_pairs_mut().append_pair("cluster", cluster);
        }

        let mut headers = HashMap::new();
        if let Some(ref token) = self.token {
            headers.insert("Authorization", token.into());
        }
        if let Some(instance_id) = self.instance_id.read().unwrap().as_deref() {
            headers.insert("fly-force-instance-id", instance_id.into());
        }
        if let Some(ref cluster_id) = self.cluster_id {
            headers.insert("Litefs-Cluster-Id", cluster_id.into());
        }

        match method {
            "GET" => {
                let resp = self.process_response(emscripten::get(u, headers))?;
                Ok(serde_json::from_slice(resp.body)?)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "Method not supported").into()),
        }
    }

    #[cfg(target_os = "emscripten")]
    fn process_response<'a, 'b>(
        &'b self,
        resp: std::result::Result<emscripten::Response<'b>, emscripten::Error>,
    ) -> Result<emscripten::Response<'b>> {
        match resp {
            Ok(resp) => {
                log::info!("all headers = {:?}", resp.headers);
                let mut instance_id = self.instance_id.write().unwrap();
                if instance_id.as_deref()
                    != resp.headers.get("lfsc-instance-id").map(|x| x.as_str())
                {
                    *instance_id = resp.headers.get("lfsc-instance-id").map(Into::into);
                    log::warn!("got new instance id {:?}", *instance_id);
                }

                Ok(resp)
            }
            Err(emscripten::Error::IO(err)) => Err(Error::Transport(err.to_string())),
            Err(emscripten::Error::Status(code, resp)) => {
                let repr: LfscErrorRepr = serde_json::from_slice(resp.body)?;
                match repr.pos {
                    Some(pos) if repr.code == "EPOSMISMATCH" => Err(Error::PosMismatch(pos)),
                    _ => Err(Error::Lfsc(LfscError {
                        http_code: code,
                        code: repr.code,
                        error: repr.error,
                    })),
                }
            }
        }
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct Info {
    #[serde(rename = "clusterID")]
    pub(crate) cluster_id: Option<String>,
}

/// A LiteFS Cloud client builder.
#[derive(Default)]
pub(crate) struct ClientBuilder {
    host: Option<url::Url>,
    token: Option<String>,
    cluster: Option<String>,
}

impl ClientBuilder {
    pub(crate) fn host(mut self, u: &url::Url) -> Self {
        self.host = Some(u.clone());
        self
    }

    pub(crate) fn token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    pub(crate) fn cluster(mut self, cluster: &str) -> Self {
        self.cluster = Some(cluster.to_string());
        self
    }

    pub(crate) fn build(self) -> Client {
        Client {
            host: self
                .host
                .unwrap_or(url::Url::parse("https://litefs.fly.io").unwrap()),
            token: self.token,
            cluster: self.cluster,
            cluster_id: None,
            instance_id: sync::RwLock::new(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Lease, Page};
    use serde_test::{assert_de_tokens, Token};

    #[test]
    fn page_de() {
        let page = Page {
            data: vec![1, 2, 3, 4, 5, 6],
            number: ltx::PageNum::new(123).unwrap(),
        };

        assert_de_tokens(
            &page,
            &[
                Token::Struct {
                    name: "Page",
                    len: 2,
                },
                Token::Str("pgno"),
                Token::U32(123),
                Token::Str("data"),
                Token::BorrowedStr("AQIDBAUG"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn lease_de() {
        use time::macros::datetime;

        let lease = Lease {
            id: "123456789".into(),
            expires_at: datetime!(2023-08-29 13:20:55.706550992 +2),
        };

        assert_de_tokens(
            &lease,
            &[
                Token::Struct {
                    name: "Lease",
                    len: 2,
                },
                Token::Str("id"),
                Token::Str("123456789"),
                Token::Str("expires_at"),
                Token::Str("2023-08-29T11:20:55.706550992Z"),
                Token::StructEnd,
            ],
        );
    }
}
