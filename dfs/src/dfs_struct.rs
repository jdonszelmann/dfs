use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::Config;
use crate::global_store::GlobalStore;
use crate::global_store::heed_store::Heed;
use crate::peer::Peer;
use crate::root::Root;
use uuid::Uuid;

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

    #[error("path for new root at {0:?} points to a file, not a folder")]
    PathIsNotDir(PathBuf),

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

    /// Get the config of the DFS
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # let td = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = td.to_path_buf();
    /// let dfs = Dfs::new(cfg.clone()).unwrap();
    /// assert_eq!(dfs.cfg(), &cfg)
    /// ```
    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    /// Adds a new peer to the DFS. Peers are global, but not all roots are shared
    /// with all peers.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # let td = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = td.to_path_buf();
    /// let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let peer = dfs.new_peer("jonathan".to_string()).unwrap();
    ///
    /// assert_eq!(peer.name(), "jonathan");
    /// ```
    ///
    /// Two peers can have the same name. Their ID and keys are then different.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # let td = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = td.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let peer1 = dfs.new_peer("jonathan").unwrap();
    /// let peer2 = dfs.new_peer("jonathan").unwrap();
    ///
    /// assert_eq!(peer1.name(), peer2.name());
    /// assert_ne!(peer1.id(), peer2.id());
    /// ```
    pub fn new_peer(&self, name: impl AsRef<str>) -> Result<Peer, NewPeerError<GS::Error>> {
        let peer = Peer::new(name.as_ref().to_string());
        self.connection.put_peer(peer.id(), &peer, false)?
            .to_err(|| NewPeerError::PeerExists)?;

        Ok(peer)
    }

    /// Adds a new root to the DFS. Roots are folders on your filesystem which are shared by
    /// some peers.
    ///
    /// TODO: nested roots
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let root = dfs.new_root(tempdir, "test").unwrap();
    ///
    /// assert_eq!(root.name(), "test");
    /// ```
    ///
    /// Two roots may not have the same name
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # let tempdir = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let peer1 = dfs.new_root(&tempdir, "jonathan").unwrap();
    /// assert!(dfs.new_root(&tempdir, "jonathan").is_err());
    /// ```
    ///
    /// The path of the root must exist and must be a folder, not a file.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # use dfs::dfs_struct::NewRootError;
    /// # let tempdir = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// assert!(matches!(
    ///     dfs.new_root(&tempdir.join("doesnt_exist"), "jonathan"),
    ///     Err(NewRootError::PathDoesntExist(_))
    /// ));
    ///
    /// // create a file we will use as  root
    /// let filepath = tempdir.join("doesnt_exist.txt");
    /// std::fs::File::create(&filepath).unwrap();
    /// assert!(matches!(
    ///     dfs.new_root(&filepath, "jonathan"),
    ///     Err(NewRootError::PathIsNotDir(_))
    /// ));
    /// ```
    pub fn new_root(&self, path: impl AsRef<Path>, name: impl AsRef<str>) -> Result<Root<GS>, NewRootError<GS::Error>> {

        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(NewRootError::PathDoesntExist(path))
        } else if !path.is_dir() {
            return Err(NewRootError::PathIsNotDir(path))
        }

        let path = path.canonicalize()?;

        let root = Root::new(self, name.as_ref().to_string(), path);

        self.connection.put_root(root.id(), &root, false)
            .map_err(NewRootError::DbInteractionError)?
            .to_err(|| NewRootError::RootExists)?;

        Ok(root)
    }

    /// Gets a root from the DFS by its name.
    /// Returns None when no root with this name exists.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let initial_root = dfs.new_root(tempdir, "test").unwrap();
    ///
    /// let root = dfs.get_root_by_name("test").unwrap().unwrap();
    ///
    /// assert_eq!(root.id(), initial_root.id());
    /// ```
    pub fn get_root_by_name(&self, name: impl AsRef<str>) -> Result<Option<Root<GS>>, GetRootError<GS::Error>> {
         Ok(
             self.connection.get_root_by_name(name.as_ref())?
             .map(|r| {
                 Root::from_storable(self, r)
             })
         )
    }

    /// Gets a root from the DFS by its uuid.
    /// Returns None when no root with this uuid exists.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let initial_root = dfs.new_root(tempdir, "test").unwrap();
    ///
    /// let root = dfs.get_root(initial_root.id()).unwrap().unwrap();
    ///
    /// assert_eq!(root.name(), initial_root.name());
    /// ```
    pub fn get_root(&self, id: Uuid) -> Result<Option<Root<GS>>, GetRootError<GS::Error>> {
        Ok(
            self.connection.get_root(id)?
                .map(|r| {
                    Root::from_storable(self, r)
                })
        )
    }

    /// Get a list of all roots
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    ///
    /// let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let initial_root = dfs.new_root(tempdir, "test").unwrap();
    ///
    /// let roots = dfs.get_roots().unwrap();
    ///
    /// assert_eq!(roots.len(), 1);
    /// assert_eq!(roots[0].name(), initial_root.name());
    /// ```
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
            let dfs = Dfs::new(cfg.clone()).unwrap();

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
            let dfs = Dfs::new(cfg.clone()).unwrap();

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
            let dfs = Dfs::new(cfg.clone()).unwrap();

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
            let dfs = Dfs::new(cfg.clone()).unwrap();


            assert!(dfs.get_root_by_name("a").unwrap().is_none());

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            let _root_b = dfs.new_root(&root_b_dir, "b").unwrap();
        }

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();
            let _a = dfs.get_root_by_name("a").unwrap();
            let _b = dfs.get_root_by_name("b").unwrap();
        }
    }
}