use crate::{http, IterLogger, OptionLogger, PositionsLogger};
use litetx as ltx;
use std::{collections::HashMap, env, fmt, io, sync};

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
    #[serde(default)]
    #[serde(with = "option_pos")]
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

#[derive(serde::Deserialize)]
struct DbChanges {
    #[serde(with = "option_pos")]
    pos: Option<ltx::Pos>,
    pgnos: Option<Vec<ltx::PageNum>>,
    all: Option<bool>,
}

/// A set of pages changed since previously known state.
#[derive(Debug)]
pub(crate) enum Changes {
    All(Option<ltx::Pos>),
    Pages(Option<ltx::Pos>, Option<Vec<ltx::PageNum>>),
}

impl Changes {
    pub(crate) fn pos(&self) -> Option<ltx::Pos> {
        match self {
            Changes::All(pos) => *pos,
            Changes::Pages(pos, _) => *pos,
        }
    }
}
impl From<DbChanges> for Changes {
    fn from(c: DbChanges) -> Changes {
        match c.all {
            Some(true) => Changes::All(c.pos),
            _ => Changes::Pages(c.pos, c.pgnos),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum LeaseOp<'a> {
    Acquire(std::time::Duration),
    Refresh(&'a str, std::time::Duration),
}

impl<'a> fmt::Display for LeaseOp<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        match self {
            LeaseOp::Acquire(dur) => write!(f, "acquire({}ms)", dur.as_millis()),
            LeaseOp::Refresh(id, dur) => write!(f, "refresh({}, {}ms)", id, dur.as_millis()),
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

        // let info = client.info()?;
        // client.set_cluster_id(if let Some(cluster_id) = info.cluster_id {
        //     cluster_id
        // } else {
        //     Client::generate_cluster_id()
        // });
        client.set_cluster_id(Client::generate_cluster_id());

        log::info!(
            "[lfsc] from_env: host = {}, cluster = {}, cluster_id = {}",
            client.host,
            OptionLogger(&client.cluster),
            OptionLogger(&client.cluster_id),
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

    pub(crate) fn pos_map(&self) -> Result<HashMap<String, Option<ltx::Pos>>> {
        log::debug!("[lfsc] pos_map");

        match self.pos_map_inner() {
            Err(err) => {
                log::error!("[lfsc] pos_map: {}", err);
                Err(err)
            }
            x => x,
        }
    }

    pub(crate) fn write_tx(
        &self,
        db: &str,
        ltx: impl io::Read,
        ltx_len: u64,
        lease: &str,
    ) -> Result<()> {
        log::debug!(
            "[lfsc] write_tx: db = {}, lease = {}, ltx_len = {}",
            db,
            lease,
            ltx_len
        );

        match self.write_tx_inner(db, ltx, ltx_len, lease) {
            Err(err) => {
                log::error!(
                    "[lfsc] write_tx: db = {}, lease = {}, ltx_len = {}: {}",
                    db,
                    lease,
                    ltx_len,
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    pub(crate) fn get_pages(
        &self,
        db: &str,
        pos: ltx::Pos,
        pgnos: &[ltx::PageNum],
    ) -> Result<Vec<Page>> {
        log::debug!(
            "[lfsc] get_pages: db = {}, pos = {}, pgnos = {}",
            db,
            pos,
            IterLogger(pgnos)
        );

        match self.get_pages_inner(db, pos, pgnos) {
            Err(err) => {
                log::error!(
                    "[lfsc] get_pages: db = {}, pos = {}, pgnos = {}: {}",
                    db,
                    pos,
                    IterLogger(pgnos),
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn info(&self) -> Result<Info> {
        log::debug!("[lfsc] info");

        match self.info_inner() {
            Err(err) => {
                log::error!("[lfsc] info: {}", err);
                Err(err)
            }
            x => x,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn sync_db(&self, db: &str, pos: Option<ltx::Pos>) -> Result<Changes> {
        log::debug!("[lfsc] sync: db = {}, pos = {}", db, OptionLogger(&pos));

        match self.sync_db_inner(db, pos) {
            Err(err) => {
                log::error!(
                    "[lfsc] sync_db: db = {}, pos = {}: {}",
                    db,
                    OptionLogger(&pos),
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn acquire_lease(&self, db: &str, op: LeaseOp) -> Result<Lease> {
        log::debug!("[lfsc] acquire_lease: db = {}, op = {}", db, op);

        match self.acquire_lease_inner(db, &op) {
            Err(err) => {
                log::error!("[lfsc] acquire_lease: db = {}, op = {}: {}", db, op, err);
                Err(err)
            }
            x => x,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn release_lease(&self, db: &str, lease: Lease) -> Result<()> {
        log::debug!("[lfsc] release_lease: db = {}, lease = {}", db, lease.id);

        match self.release_lease_inner(db, &lease) {
            Err(err) => {
                log::error!(
                    "[lfsc] release_lease: db = {}, lease = {}: {}",
                    db,
                    lease.id,
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    pub(crate) fn sync(
        &self,
        positions: &HashMap<String, Option<ltx::Pos>>,
    ) -> Result<HashMap<String, Changes>> {
        log::debug!("[lfsc] sync: positions = {}", PositionsLogger(positions));

        match self.sync_inner(positions) {
            Err(err) => {
                log::error!(
                    "[lfsc] sync: positions = {}: {}",
                    PositionsLogger(positions),
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    fn pos_map_inner(&self) -> Result<HashMap<String, Option<ltx::Pos>>> {
        let mut u = self.host.clone();
        u.set_path("/pos");

        #[derive(serde::Deserialize)]
        #[serde(transparent)]
        struct Helper(#[serde(with = "option_pos")] Option<ltx::Pos>);

        Ok(self
            .call::<HashMap<String, Helper>>("GET", u)?
            .into_iter()
            .map(|(k, v)| (k, v.0))
            .collect())
    }

    fn write_tx_inner(
        &self,
        db: &str,
        ltx: impl io::Read,
        ltx_len: u64,
        lease: &str,
    ) -> Result<()> {
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

    fn get_pages_inner(
        &self,
        db: &str,
        pos: ltx::Pos,
        pgnos: &[ltx::PageNum],
    ) -> Result<Vec<Page>> {
        #[derive(serde::Deserialize)]
        struct GetPageResponse {
            pages: Vec<Page>,
        }

        let mut u = self.host.clone();
        u.set_path("/db/page");
        u.query_pairs_mut()
            .append_pair("db", db)
            .append_pair("pos", &pos.to_string())
            .append_pair(
                "pgno",
                &pgnos
                    .iter()
                    .map(|pgno| pgno.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );

        Ok(self.call::<GetPageResponse>("GET", u)?.pages)
    }

    #[allow(dead_code)]
    fn info_inner(&self) -> Result<Info> {
        let mut u = self.host.clone();
        u.set_path("/info");

        self.call("GET", u)
    }

    fn sync_db_inner(&self, db: &str, pos: Option<ltx::Pos>) -> Result<Changes> {
        let mut u = self.host.clone();
        u.set_path("/db/sync");
        u.query_pairs_mut().append_pair("db", db);
        if let Some(pos) = pos {
            u.query_pairs_mut().append_pair("pos", &pos.to_string());
        }

        Ok(self.call::<DbChanges>("GET", u)?.into())
    }

    #[allow(dead_code)]
    fn acquire_lease_inner(&self, db: &str, op: &LeaseOp) -> Result<Lease> {
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

    #[allow(dead_code)]
    fn release_lease_inner(&self, db: &str, lease: &Lease) -> Result<()> {
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

    fn sync_inner(
        &self,
        positions: &HashMap<String, Option<ltx::Pos>>,
    ) -> Result<HashMap<String, Changes>> {
        let mut u = self.host.clone();
        u.set_path("/sync");

        #[derive(serde::Serialize)]
        #[serde(transparent)]
        struct Helper(#[serde(with = "option_pos")] Option<ltx::Pos>);

        #[derive(serde::Serialize)]
        struct SyncRequest<'a> {
            positions: HashMap<&'a str, Helper>,
        }

        #[derive(serde::Deserialize)]
        struct SyncResponse {
            changes: HashMap<String, DbChanges>,
        }

        let positions: HashMap<&str, Helper> = positions
            .iter()
            .map(|(k, &v)| (k.as_str(), Helper(v)))
            .collect();

        let req = self.make_request("POST", u);
        let resp = self.process_response(req.send_json(SyncRequest { positions }))?;
        let resp = resp.into_json::<SyncResponse>()?;

        Ok(resp
            .changes
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect())
    }

    fn call<R>(&self, method: &str, u: url::Url) -> Result<R>
    where
        R: serde::de::DeserializeOwned,
    {
        let req = self.make_request(method, u);
        let resp = self.process_response(req.call())?;

        Ok(resp.into_json()?)
    }

    fn make_request(&self, method: &str, mut u: url::Url) -> http::Request {
        if let Some(ref cluster) = self.cluster {
            u.query_pairs_mut().append_pair("cluster", cluster);
        }

        let mut req = http::Request::new(method, &u);
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

    fn process_response(
        &self,
        resp: std::result::Result<http::Response, http::Error>,
    ) -> Result<http::Response> {
        match resp {
            Ok(resp) => {
                let mut instance_id = self.instance_id.write().unwrap();
                if instance_id.as_deref() != resp.header("Lfsc-Instance-Id") {
                    *instance_id = resp.header("Lfsc-Instance-Id").map(Into::into);
                }

                Ok(resp)
            }
            Err(http::Error::Transport(err)) => Err(Error::Transport(err)),
            Err(http::Error::Status(code, body)) => {
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
}

#[allow(dead_code)]
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

mod option_pos {
    use litetx as ltx;
    use serde::{
        de::{self, Deserializer},
        ser::{Serialize, SerializeStruct, Serializer},
        Deserialize,
    };

    pub fn serialize<S>(value: &Option<ltx::Pos>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(pos) => pos.serialize(serializer),
            None => {
                let mut state = serializer.serialize_struct("Pos", 2)?;
                state.serialize_field("txid", "0000000000000000")?;
                state.serialize_field("postApplyChecksum", "0000000000000000")?;
                state.end()
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ltx::Pos>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            txid: String,
            #[serde(rename = "postApplyChecksum")]
            post_apply_checksum: String,
        }

        let helper: Helper = Deserialize::deserialize(deserializer)?;
        let txid = u64::from_str_radix(&helper.txid, 16).map_err(de::Error::custom)?;
        let post_apply_checksum =
            u64::from_str_radix(&helper.post_apply_checksum, 16).map_err(de::Error::custom)?;

        match (txid, post_apply_checksum) {
            (0, 0) => Ok(None),
            (t, p) => Ok(Some(ltx::Pos {
                txid: ltx::TXID::new(t).map_err(de::Error::custom)?,
                post_apply_checksum: ltx::Checksum::new(p),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Lease, Page};
    use litetx as ltx;
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
