use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::Config;
use crate::global_store::GlobalStore;
use crate::global_store::heed_store::Heed;
use crate::peer::Peer;
use crate::root::Root;

#[derive(Debug, Error)]
pub enum NewDfsError<GSE> {
    #[error("db error: {0}")]
    DbInteractionError(#[from] GSE)
}

#[derive(Debug, Error)]
pub enum NewPeerError<GSE> {
    #[error("db error: {0}")]
    Sqlite(#[from] GSE),

    #[error("peer with this name already exists")]
    PeerExists
}

#[derive(Debug, Error)]
pub enum GetRootError <GSE> {
    #[error("failed to compile or run sql statement: {0}")]
    CompileStatement(#[from] GSE),
}

#[derive(Debug, Error)]
pub enum NewRootError<GSE> {
    #[error("db interaction error: {0}")]
    DbInteractionError(GSE),

    #[error("failed to canonicalize path: {0}")]
    CanonicalizePath(#[from] io::Error),

    #[error("path wasn't properly encoded utf8")]
    Utf8,

    #[error("path for new root at {0:?} doesn't point to an existing folder (please make it first)")]
    PathDoesntExist(PathBuf),

    #[error("root with this name already exists")]
    RootExists
}


pub struct Dfs<GS = Heed>{
    cfg: Config,
    pub(crate) connection: GS,
}

impl Dfs<Heed> {
    pub fn new(cfg: Config) -> Result<Self, NewDfsError<<Heed as GlobalStore>::Error>> {
        Self::new_internal(cfg)
    }
}

impl<GS: GlobalStore> Dfs<GS> {
    fn new_internal(cfg: Config) -> Result<Self, NewDfsError<GS::Error>> {
        Ok(Self {
            connection: GS::new(&cfg.global_db)?,
            cfg,
        })
    }

    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    pub fn new_peer(&self, name: String) -> Result<Peer, NewPeerError<GS::Error>> {
        let peer = Peer::new(name);
        self.connection.put_peer(peer.id(), &peer, false)?;

        Ok(peer)
    }

    pub fn new_root(&mut self, path: impl AsRef<Path>, name: impl AsRef<str>) -> Result<Root<GS>, NewRootError<GS::Error>> {

        let path = path.as_ref().to_path_buf().canonicalize()?;

        if !path.exists() {
            return Err(NewRootError::PathDoesntExist(path))
        }

        let root = Root::new(self, name.as_ref().to_string(), path);

        self.connection.put_root(root.id(), &root, false)
            .map_err(NewRootError::DbInteractionError)?
            .to_err(|| NewRootError::RootExists)?;

        Ok(root)
    }

    pub fn get_root(&self, name: impl AsRef<str>) -> Result<Option<Root<GS>>, GetRootError<GS::Error>> {
         Ok(
             self.connection.get_root_by_name(name.as_ref())?
             .map(|r| {
                 Root::from_storable(self, r)
             })
         )
    }

    pub fn get_roots(&self) -> Result<Vec<Root<GS>>, GetRootError<GS::Error>> {
        Ok(
            self.connection.get_all_roots()?
                .into_iter()
                .map(|s| Root::from_storable(self, s))
                .collect()
        )
    }
}

#[cfg(test)]
mod tests {
    use temp_testdir::TempDir;

    use crate::config::Config;
    use crate::Dfs;

    #[test]
    fn root_same_path() {
        let root_a_dir = TempDir::new("test a", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        {
            let mut dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            assert!(dfs.new_root(root_a_dir, "a").is_err());
        }
    }

    #[test]
    fn root_same_name() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        {
            let mut dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_b = dfs.new_root(&root_a_dir, "a").unwrap();
            assert!(dfs.new_root(root_b_dir, "a").is_err());
        }
    }

    #[test]
    fn root_different_name() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        {
            let mut dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            let _root_b = dfs.new_root(&root_b_dir, "b").unwrap();
        }
    }

    #[test]
    fn persistent_root() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        {
            let mut dfs = Dfs::new(cfg.clone()).unwrap();


            assert!(dfs.get_root("a").unwrap().is_none());

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            let _root_b = dfs.new_root(&root_b_dir, "b").unwrap();
        }

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();
            let _a = dfs.get_root("a").unwrap();
            let _b = dfs.get_root("b").unwrap();
        }
    }
}