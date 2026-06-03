//! TOML config-file support for `wakunode`.
//!
//! A `--config <file>` provides values that fill in wherever the corresponding
//! CLI flag was left unset; CLI flags always take precedence, and built-in
//! defaults apply where neither is given.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Node settings loadable from a TOML file. All fields are optional; absent
/// fields fall back to the CLI flag (or its default).
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
pub struct FileConfig {
    pub cluster_id: Option<u16>,
    pub shard: Vec<u16>,
    pub tcp_port: Option<u16>,
    pub staticnode: Vec<String>,
    pub discv5_discovery: Option<bool>,
    pub discv5_udp_port: Option<u16>,
    pub ext_ip: Option<String>,
    pub discv5_bootstrap_node: Vec<String>,
    pub dns_discovery: Option<bool>,
    pub dns_discovery_url: Vec<String>,
    pub store: Option<bool>,
    pub store_path: Option<String>,
    pub node_key_file: Option<PathBuf>,
    pub rest_port: Option<u16>,
    pub max_connections: Option<u32>,
    pub ip_colocation_limit: Option<usize>,
}

impl FileConfig {
    /// Parse a config file. Returns the default (empty) config if `path` is `None`.
    pub fn load(path: Option<&Path>) -> Result<Self, String> {
        match path {
            None => Ok(Self::default()),
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .map_err(|e| format!("reading {}: {e}", path.display()))?;
                toml::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_a_toml_config() {
        let toml = r#"
            cluster-id = 1
            shard = [0, 1, 2]
            tcp-port = 60000
            dns-discovery = true
            store-path = "/data/store.db"
            rest-port = 8645
            max-connections = 512
        "#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.cluster_id, Some(1));
        assert_eq!(cfg.shard, vec![0, 1, 2]);
        assert_eq!(cfg.tcp_port, Some(60000));
        assert_eq!(cfg.dns_discovery, Some(true));
        assert_eq!(cfg.store_path.as_deref(), Some("/data/store.db"));
        assert_eq!(cfg.rest_port, Some(8645));
        assert_eq!(cfg.max_connections, Some(512));
        // Unset fields stay None / empty.
        assert_eq!(cfg.node_key_file, None);
        assert!(cfg.staticnode.is_empty());
    }

    #[test]
    fn missing_path_is_default() {
        assert_eq!(FileConfig::load(None).unwrap(), FileConfig::default());
    }

    #[test]
    fn loads_from_disk() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "rest-port = 9999").unwrap();
        let cfg = FileConfig::load(Some(f.path())).unwrap();
        assert_eq!(cfg.rest_port, Some(9999));
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<FileConfig>("bogus-key = 1").is_err());
    }
}
