use std::path::PathBuf;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Config {
    pub local_db: PathBuf,
    pub global_db: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        let mut data_dir: PathBuf = std::env::var("XDG_DATA_HOME")
            .unwrap_or_else(|_| "~/.local/share".into())
            .into();

        data_dir.push("dfs");

        Self {
            local_db: ".dfs".into(),
            global_db: data_dir
        }
    }
}

pub mod test {
    use temp_testdir::TempDir;
    use crate::config::Config;

    impl Config {
        /// For use in tests. Makes paths in-memory and more.
        pub fn test_config(dir: &TempDir) -> Self {
            Config {
                global_db: dir.to_path_buf(),
                ..Default::default()
            }
        }
    }
}
