//! Startup configuration.
//!
//! Two layers, in precedence order: a TOML file, then environment variables.
//! The file holds everything shareable (chain id, pool list, tuning); the
//! environment holds anything secret (database URL, RPC endpoints with API keys
//! in them). That split is why `chainscope.toml` is committed and `.env` is not.
//!
//! Loading is a two-step: deserialize into `RawConfig`, where every field is
//! optional and every address is still a string, then `validate` it into
//! `Config`, where nothing is optional and an address is 20 bytes. Downstream
//! code takes `&Config` and cannot ask "is this set?" or "does this parse?",
//! because by then those questions have already been answered. Configuration
//! errors are only ever allowed to happen at startup — never at runtime.

use std::fmt;

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Deserializer};

/// Default config file, relative to the working directory.
const DEFAULT_PATH: &str = "chainscope.toml";

// Defaults live here rather than in serde attributes so every fallback value is
// visible in one place next to the bound that validates it.
const DEFAULT_MAX_CONNECTIONS: u32 = 5;
const DEFAULT_FINALITY_DEPTH: u64 = 64; // ~2 epochs on Ethereum
const DEFAULT_CHANNEL_CAPACITY: usize = 1_024;
const DEFAULT_BATCH_SIZE: usize = 500;
const DEFAULT_BACKFILL_CHUNK: u64 = 2_000;
const DEFAULT_LOG_FILTER: &str = "info";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not read configuration: {0}")]
    Source(#[from] figment::Error),

    #[error("{field} is not set. {hint}")]
    Missing { field: String, hint: String },

    #[error("{field}: {problem} (got `{value}`)")]
    Invalid {
        field: String,
        value: String,
        problem: String,
    },
}

impl ConfigError {
    fn missing(field: &str, hint: &str) -> Self {
        Self::Missing {
            field: field.into(),
            hint: hint.into(),
        }
    }

    fn invalid(field: &str, value: impl fmt::Display, problem: &str) -> Self {
        Self::Invalid {
            field: field.into(),
            value: value.to_string(),
            problem: problem.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Address
// ---------------------------------------------------------------------------

/// A 20-byte EVM address.
///
/// Stored as bytes, not as a hex string, so that a value of this type is proof
/// the address was well formed. The database columns are `BYTEA` for the same
/// reason.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Address(pub [u8; 20]);

impl Address {
    fn parse(s: &str) -> Result<Self, &'static str> {
        let body = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
        if body.len() != 40 {
            return Err("an address is 40 hex characters after the 0x prefix");
        }
        let mut bytes = [0u8; 20];
        hex::decode_to_slice(body, &mut bytes).map_err(|_| "contains non-hex characters")?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

// ---------------------------------------------------------------------------
// Raw shape (what the file and environment actually provide)
// ---------------------------------------------------------------------------

/// Accepts either a TOML array or a comma-separated string.
///
/// Lists are the one shape that does not survive the trip through an
/// environment variable, and `CHAINSCOPE_CHAIN__POOLS=0xaaa...,0xbbb...` is how
/// anyone would expect to write one.
#[derive(Debug, Clone)]
struct StringList(Vec<String>);

impl<'de> Deserialize<'de> for StringList {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            List(Vec<String>),
            Csv(String),
        }

        Ok(match Either::deserialize(d)? {
            Either::List(v) => StringList(v),
            Either::Csv(s) => StringList(
                s.split(',')
                    .map(str::trim)
                    .filter(|p| !p.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ),
        })
    }
}

// `deny_unknown_fields` turns a typo in the TOML file — or a stray
// CHAINSCOPE_* variable — into a startup error instead of a setting that
// silently does nothing.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    database: RawDatabase,
    #[serde(default)]
    chain: RawChain,
    #[serde(default)]
    pipeline: RawPipeline,
    #[serde(default)]
    log: RawLog,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatabase {
    url: Option<String>,
    max_connections: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChain {
    chain_id: Option<u64>,
    rpc_endpoints: Option<StringList>,
    factory: Option<String>,
    pools: Option<StringList>,
    start_block: Option<u64>,
    finality_depth: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPipeline {
    channel_capacity: Option<usize>,
    batch_size: Option<usize>,
    backfill_chunk_size: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLog {
    filter: Option<String>,
}

// ---------------------------------------------------------------------------
// Validated shape (what the rest of the program sees)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Config {
    pub database: Database,
    pub chain: Chain,
    pub pipeline: Pipeline,
    pub log: Log,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone)]
pub struct Chain {
    pub chain_id: u64,
    pub rpc_endpoints: Vec<url::Url>,
    pub factory: Address,
    pub pools: Vec<Address>,
    pub start_block: u64,
    pub finality_depth: u64,
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub backfill_chunk_size: u64,
}

#[derive(Debug, Clone)]
pub struct Log {
    pub filter: String,
}

impl Config {
    /// Load from `chainscope.toml` plus the environment.
    pub fn load() -> Result<Self, ConfigError> {
        Self::from_figment(Self::figment(DEFAULT_PATH))
    }

    /// A missing TOML file is not an error — every value it would supply can
    /// come from the environment instead, which is how this runs in Docker.
    fn figment(path: &str) -> Figment {
        let mut f = Figment::new().merge(Toml::file(path));

        // DATABASE_URL is the conventional unprefixed name and is what the
        // sqlx CLI reads, so it is honoured directly rather than forcing a
        // second CHAINSCOPE_-prefixed copy of the same secret.
        if let Ok(url) = std::env::var("DATABASE_URL") {
            f = f.merge(Serialized::default("database.url", url));
        }

        // Merged last, so an explicit CHAINSCOPE_* variable beats both.
        f.merge(Env::prefixed("CHAINSCOPE_").split("__"))
    }

    fn from_figment(f: Figment) -> Result<Self, ConfigError> {
        Self::validate(f.extract::<RawConfig>()?)
    }

    /// Turn "maybe present, maybe well formed" into "present and well formed".
    ///
    /// Every branch names the field and echoes the offending value, because the
    /// person reading this message is looking at a process that just refused to
    /// start and needs to know which line to edit.
    fn validate(raw: RawConfig) -> Result<Self, ConfigError> {
        // --- database ---
        let url = raw.database.url.filter(|u| !u.trim().is_empty()).ok_or_else(|| {
            ConfigError::missing(
                "database.url",
                "Set DATABASE_URL in .env, or database.url in chainscope.toml.",
            )
        })?;
        match url::Url::parse(&url) {
            Ok(u) if matches!(u.scheme(), "postgres" | "postgresql") => {}
            Ok(u) => {
                return Err(ConfigError::invalid(
                    "database.url",
                    redact_url(&url),
                    &format!("scheme must be postgres:// or postgresql://, not {}://", u.scheme()),
                ))
            }
            Err(e) => {
                return Err(ConfigError::invalid(
                    "database.url",
                    redact_url(&url),
                    &format!("not a valid URL: {e}"),
                ))
            }
        }

        let max_connections = raw.database.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
        bound("database.max_connections", max_connections as u64, 1, 100)?;

        // --- chain ---
        let chain_id = raw.chain.chain_id.ok_or_else(|| {
            ConfigError::missing("chain.chain_id", "Ethereum mainnet is 1.")
        })?;
        if chain_id == 0 {
            return Err(ConfigError::invalid("chain.chain_id", chain_id, "must be non-zero"));
        }

        let raw_endpoints = raw.chain.rpc_endpoints.map(|l| l.0).unwrap_or_default();
        if raw_endpoints.is_empty() {
            return Err(ConfigError::missing(
                "chain.rpc_endpoints",
                "At least one RPC endpoint is required; two or three give failover.",
            ));
        }
        let mut rpc_endpoints = Vec::with_capacity(raw_endpoints.len());
        for (i, ep) in raw_endpoints.iter().enumerate() {
            let field = format!("chain.rpc_endpoints[{i}]");
            let parsed = url::Url::parse(ep)
                .map_err(|e| ConfigError::invalid(&field, redact_url(ep), &format!("not a valid URL: {e}")))?;
            if !matches!(parsed.scheme(), "http" | "https" | "ws" | "wss") {
                return Err(ConfigError::invalid(
                    &field,
                    redact_url(ep),
                    &format!("scheme must be http, https, ws or wss, not {}", parsed.scheme()),
                ));
            }
            rpc_endpoints.push(parsed);
        }

        let factory_raw = raw.chain.factory.ok_or_else(|| {
            ConfigError::missing(
                "chain.factory",
                "The Uniswap V3 factory on mainnet is 0x1f98431c8ad98523631ae4a59f267346ea31f984.",
            )
        })?;
        let factory = Address::parse(&factory_raw)
            .map_err(|why| ConfigError::invalid("chain.factory", &factory_raw, why))?;

        let raw_pools = raw.chain.pools.map(|l| l.0).unwrap_or_default();
        if raw_pools.is_empty() {
            return Err(ConfigError::missing(
                "chain.pools",
                "List at least one pool address to index.",
            ));
        }
        let mut pools = Vec::with_capacity(raw_pools.len());
        for (i, p) in raw_pools.iter().enumerate() {
            let addr = Address::parse(p)
                .map_err(|why| ConfigError::invalid(&format!("chain.pools[{i}]"), p, why))?;
            if pools.contains(&addr) {
                return Err(ConfigError::invalid(
                    &format!("chain.pools[{i}]"),
                    addr,
                    "listed more than once",
                ));
            }
            pools.push(addr);
        }

        let start_block = raw.chain.start_block.unwrap_or(0);
        // Ethereum is at ~2.3e7 blocks. Anything past a billion is a typo, and
        // catching it here beats a backfill that silently indexes nothing.
        bound("chain.start_block", start_block, 0, 1_000_000_000)?;

        let finality_depth = raw.chain.finality_depth.unwrap_or(DEFAULT_FINALITY_DEPTH);
        bound("chain.finality_depth", finality_depth, 1, 100_000)?;

        // --- pipeline ---
        let channel_capacity = raw.pipeline.channel_capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY);
        bound("pipeline.channel_capacity", channel_capacity as u64, 1, 1_048_576)?;

        let batch_size = raw.pipeline.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        bound("pipeline.batch_size", batch_size as u64, 1, 100_000)?;

        let backfill_chunk_size = raw.pipeline.backfill_chunk_size.unwrap_or(DEFAULT_BACKFILL_CHUNK);
        bound("pipeline.backfill_chunk_size", backfill_chunk_size, 1, 100_000)?;

        // --- log ---
        let filter = raw.log.filter.unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned());

        Ok(Config {
            database: Database { url, max_connections },
            chain: Chain {
                chain_id,
                rpc_endpoints,
                factory,
                pools,
                start_block,
                finality_depth,
            },
            pipeline: Pipeline {
                channel_capacity,
                batch_size,
                backfill_chunk_size,
            },
            log: Log { filter },
        })
    }

    /// A human-readable dump with every secret removed, logged once at startup.
    ///
    /// Worth logging because "which config did this process actually end up
    /// with" is otherwise unanswerable across two layers of overrides.
    pub fn summary(&self) -> String {
        let endpoints: Vec<String> = self
            .chain
            .rpc_endpoints
            .iter()
            .map(|u| redact_url(u.as_str()))
            .collect();

        format!(
            "database.url={} max_connections={} | chain_id={} rpc_endpoints=[{}] factory={} pools={} \
             start_block={} finality_depth={} | channel_capacity={} batch_size={} backfill_chunk_size={} | log={}",
            redact_url(&self.database.url),
            self.database.max_connections,
            self.chain.chain_id,
            endpoints.join(", "),
            self.chain.factory,
            self.chain.pools.len(),
            self.chain.start_block,
            self.chain.finality_depth,
            self.pipeline.channel_capacity,
            self.pipeline.batch_size,
            self.pipeline.backfill_chunk_size,
            self.log.filter,
        )
    }
}

fn bound(field: &str, value: u64, min: u64, max: u64) -> Result<(), ConfigError> {
    if value < min || value > max {
        return Err(ConfigError::invalid(
            field,
            value,
            &format!("must be between {min} and {max}"),
        ));
    }
    Ok(())
}

/// Strip the two places a URL hides a secret: the password, and the path or
/// query where RPC providers put API keys.
///
/// Used for every URL that reaches a log line or an error message, so a pasted
/// stack trace never leaks an endpoint key.
fn redact_url(raw: &str) -> String {
    let Ok(mut u) = url::Url::parse(raw) else {
        return "<unparseable url>".to_owned();
    };

    if u.password().is_some() {
        let _ = u.set_password(Some("***"));
    }

    // Providers put API keys in either the path (Alchemy: /v2/KEY) or the query
    // string (Infura-style ?key=). Neither is worth keeping in a log line, so
    // anything after the host goes.
    if u.query().is_some() || !u.path().trim_matches('/').is_empty() {
        u.set_query(None);
        // ASCII on purpose: the URL crate percent-encodes anything else, and
        // "/%E2%80%A6" in a log line looks like a bug rather than a redaction.
        u.set_path("/redacted");
    }

    u.to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use figment::providers::Format;

    const POOL: &str = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
    const FACTORY: &str = "0x1f98431c8ad98523631ae4a59f267346ea31f984";

    fn toml_config(body: &str) -> Result<Config, ConfigError> {
        Config::from_figment(Figment::new().merge(Toml::string(body)))
    }

    fn valid_toml() -> String {
        format!(
            r#"
            [database]
            url = "postgres://u:p@localhost:5432/chainscope"

            [chain]
            chain_id = 1
            rpc_endpoints = ["https://eth.example/v1/KEY"]
            factory = "{FACTORY}"
            pools = ["{POOL}"]
            "#
        )
    }

    #[test]
    fn valid_config_loads_with_defaults_filled_in() {
        let c = toml_config(&valid_toml()).expect("should load");
        assert_eq!(c.chain.chain_id, 1);
        assert_eq!(c.chain.pools.len(), 1);
        assert_eq!(c.chain.factory.to_string(), FACTORY);
        // Untouched sections fall back to the documented defaults.
        assert_eq!(c.chain.finality_depth, DEFAULT_FINALITY_DEPTH);
        assert_eq!(c.pipeline.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.database.max_connections, DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn malformed_pool_address_names_the_field_and_the_value() {
        let bad = valid_toml().replace(POOL, "0xdeadbeef");
        let err = toml_config(&bad).unwrap_err().to_string();
        assert!(err.contains("chain.pools[0]"), "should name the field: {err}");
        assert!(err.contains("0xdeadbeef"), "should echo the value: {err}");
        assert!(err.contains("40 hex"), "should say what is wrong: {err}");
    }

    #[test]
    fn non_hex_address_is_rejected() {
        let bad = valid_toml().replace(POOL, "0xzzzz99c3a0ff1de082011efddc58f1908eb6e6d8");
        let err = toml_config(&bad).unwrap_err().to_string();
        assert!(err.contains("non-hex"), "{err}");
    }

    #[test]
    fn missing_database_url_is_reported_before_anything_else() {
        let body = valid_toml().replace(r#"url = "postgres://u:p@localhost:5432/chainscope""#, "");
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("database.url is not set"), "{err}");
        assert!(err.contains("DATABASE_URL"), "should say how to fix it: {err}");
    }

    #[test]
    fn wrong_database_scheme_is_rejected() {
        let body = valid_toml().replace("postgres://", "mysql://");
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("database.url"), "{err}");
        assert!(err.contains("postgres://"), "{err}");
    }

    #[test]
    fn empty_pool_list_is_rejected() {
        let body = valid_toml().replace(&format!(r#"["{POOL}"]"#), "[]");
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("chain.pools is not set"), "{err}");
    }

    #[test]
    fn duplicate_pool_is_rejected() {
        let body = valid_toml().replace(&format!(r#"["{POOL}"]"#), &format!(r#"["{POOL}", "{POOL}"]"#));
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("chain.pools[1]"), "{err}");
        assert!(err.contains("more than once"), "{err}");
    }

    #[test]
    fn bad_rpc_scheme_is_rejected() {
        let body = valid_toml().replace("https://eth.example/v1/KEY", "ftp://eth.example");
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("chain.rpc_endpoints[0]"), "{err}");
        assert!(err.contains("ftp"), "{err}");
    }

    #[test]
    fn out_of_range_numbers_are_rejected() {
        let body = format!("{}\n[pipeline]\nbatch_size = 0\n", valid_toml());
        let err = toml_config(&body).unwrap_err().to_string();
        assert!(err.contains("pipeline.batch_size"), "{err}");
        assert!(err.contains("between 1 and 100000"), "{err}");
    }

    #[test]
    fn unknown_key_is_rejected_rather_than_ignored() {
        let body = format!("{}\n[chain]\nnonsense = 1\n", valid_toml());
        assert!(toml_config(&body).is_err());
    }

    #[test]
    fn comma_separated_list_is_accepted_for_environment_use() {
        let other = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640";
        let body = valid_toml().replace(&format!(r#"["{POOL}"]"#), &format!(r#""{POOL}, {other}""#));
        let c = toml_config(&body).expect("csv list should parse");
        assert_eq!(c.chain.pools.len(), 2);
    }

    #[test]
    fn summary_hides_the_database_password_and_the_rpc_key() {
        let c = toml_config(&valid_toml()).unwrap();
        let s = c.summary();
        assert!(!s.contains(":p@"), "password leaked: {s}");
        assert!(!s.contains("KEY"), "rpc api key leaked: {s}");
        assert!(s.contains("***"), "{s}");
    }

    #[test]
    fn environment_overrides_the_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("chainscope.toml", &valid_toml())?;
            jail.set_env("CHAINSCOPE_CHAIN__CHAIN_ID", "11155111");
            jail.set_env("CHAINSCOPE_PIPELINE__BATCH_SIZE", "42");

            let c = Config::load().expect("should load");
            assert_eq!(c.chain.chain_id, 11155111);
            assert_eq!(c.pipeline.batch_size, 42);
            Ok(())
        });
    }

    #[test]
    fn database_url_env_var_is_honoured_without_a_prefix() {
        figment::Jail::expect_with(|jail| {
            let body = valid_toml().replace(r#"url = "postgres://u:p@localhost:5432/chainscope""#, "");
            jail.create_file("chainscope.toml", &body)?;
            jail.set_env("DATABASE_URL", "postgres://env:secret@db:5432/chainscope");

            let c = Config::load().expect("should load");
            assert!(c.database.url.contains("db:5432"));
            Ok(())
        });
    }
}
