use std::{collections::HashMap, env, fmt, io, sync};

/// All possible errors returned by the LFSC client.
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("transport level: {0}")]
    Transport(Box<ureq::Transport>),
    #[error("ltx position mismatch: {0}")]
    PosMismatch(ltx::Pos),
    #[error("LFSC: {0}")]
    Lfsc(LfscError),
    #[error("body")]
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

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub(crate) struct LfscError {
    http_code: u16,
    code: String,
    error: String,
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
    instance_id: sync::RwLock<Option<String>>,
}

impl Client {
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

        Ok(builder.build())
    }

    pub(crate) fn pos_map(&self) -> Result<HashMap<String, ltx::Pos>> {
        log::debug!("[lfsc] pos_map");

        let mut u = self.host.clone();
        u.set_path("/pos");

        let req = self.make_request("GET", u);
        let resp = self.process_response(req.call())?;

        Ok(resp.into_json()?)
    }

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

        req
    }

    fn process_response(
        &self,
        resp: std::result::Result<ureq::Response, ureq::Error>,
    ) -> Result<ureq::Response> {
        match resp {
            Ok(resp) => {
                let update_instance_id =
                    self.instance_id.read().unwrap().as_deref() != resp.header("Lfsc-Instance-Id");
                if update_instance_id {
                    *self.instance_id.write().unwrap() =
                        resp.header("Lfsc-Instance-Id").map(Into::into);
                }

                Ok(resp)
            }
            Err(ureq::Error::Transport(err)) => Err(Error::Transport(Box::new(err))),
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
            instance_id: sync::RwLock::new(None),
        }
    }
}
