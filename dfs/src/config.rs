use std::path::PathBuf;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub local_db: Option<PathBuf>,
    pub global_db: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        let mut data_dir: PathBuf = std::env::var("XDG_DATA_HOME")
            .unwrap_or_else(|_| "~/.local/share".into())
            .into();

        data_dir.push("dfs");

        Self {
            local_db: Some(".dfs".into()),
            global_db: Some(data_dir,)
        }
    }
}

impl Config {
    /// For use in tests. Makes paths in-memory and more.
    pub fn test_config() -> Self {
        Config {
            global_db: None,
            ..Default::default()
        }
    }
}