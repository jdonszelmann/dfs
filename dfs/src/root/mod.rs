use std::fs::create_dir_all;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

use thiserror::Error;

use dir_entry::DirEntry;

use crate::Dfs;
use crate::root::index::{IndexError, Indexer};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use crate::global_store::GlobalStore;
use crate::root::local_store::heed_store::Heed;
use crate::root::local_store::LocalStore;
use crate::root::local_store::sled_store::Sled;

pub mod index;
pub mod dir_entry;
pub mod local_store;


#[derive(Debug, Error)]
pub enum DbConnectionError<LSE> {

    #[error("db error: {0}")]
    DbInteractionError(#[from] LSE),

    #[error("the folder this root is associated with does not exist (anymore). The folder was {0:?}")]
    RootPathDoesntExist(PathBuf),

    #[error("the path of this dfs root points to a file, not a folder ({0:?})")]
    PathIsFile(PathBuf),

    #[error("failed to create the .dfs folder in {0:?}: {1}")]
    CreateFolder(PathBuf, io::Error),
}

#[derive(Debug, Error)]
pub enum GetDirEntryError<LSE> {
    #[error("db error: {0}")]
    DbInteractionError(#[from] LSE),
}

#[derive(Debug, Error)]
pub enum GetRootEntryError<LSE> {
    #[error("db error: {0}")]
    DbInteractionError(#[from] LSE),

    #[error("path of root doesn't exist")]
    Exists(PathBuf),

    #[error("path of root doesn't point to a directory")]
    NotDir(PathBuf),
}

/// A StorableRoot defines the subset of fields in a root which
/// can be stored in the [`GlobalStore`].
///
/// Refer to [`Root`] for further documentation.
#[derive(Serialize, Deserialize)]
pub struct StorableRoot {
    uuid: Uuid,
    path: PathBuf,
    name: String,
    root_direntry_id: Option<Uuid>,
}

impl StorableRoot {
    /// Get the path of a root.
    /// All files in a root have a path relative to this path.
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let root = dfs.new_root(&tempdir, "test").unwrap();
    /// assert_eq!(root.path(), &tempdir.as_ref().canonicalize().unwrap())
    /// ```
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get the name of this root.
    /// The name is next to the uuid a unique identifier of a root.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let root = dfs.new_root(&tempdir, "test").unwrap();
    /// assert_eq!(root.name(), "test")
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the uuid of this root.
    /// The uuid is next to the name a unique identifier of a root.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let root = dfs.new_root(&tempdir, "test").unwrap();
    /// assert_eq!(root.name(), "test")
    /// ```
    pub fn id(&self) -> Uuid {
        self.uuid
    }
}

/// A Root is a collection of files and folders which are shared with
/// some peers. A root has a path (to a folder!) and all subdirectories
/// and files are shared with the peers of a root.
///
/// A Root dereferences to a [`StorableRoot`]. [`StorableRoot`]s cannot be
/// used on their own, but are the part of a root stored in the [`GlobalStore`].
pub struct Root<'dfs, GS> {
    storable: StorableRoot,
    dfs: &'dfs Dfs<GS>,
}

impl<'dfs, GS> Deref for Root<'dfs, GS> {
    type Target = StorableRoot;

    fn deref(&self) -> &Self::Target {
        &self.storable
    }
}

impl<'dfs, GS> DerefMut for Root<'dfs, GS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storable
    }
}

impl<'dfs, GS: GlobalStore> Root<'dfs, GS> {
    /// Create a root from a [`StorableRoot`]. To Create a root, use
    /// the [`create_root`] function on a [`Dfs`]
    pub(crate) fn from_storable(dfs: &'dfs Dfs<GS>, storable: StorableRoot) -> Self {
        Self {
            dfs,
            storable,
        }
    }

    /// Create a root from a [`StorableRoot`] To Create a root, use
    /// the [`create_root`] function on a [`Dfs`]
    pub(crate) fn new(dfs: &'dfs Dfs<GS>, name: String, path: PathBuf) -> Self {
        let uuid = Uuid::new_v4();

        Self::from_storable(dfs, StorableRoot {
            uuid,
            name,
            path,
            root_direntry_id: None
        })
    }

    /// By default, Roots are disconnected from their [`LocalStore`]. By connecting
    /// a Root, this [`LocalStore`] is opened, and files in the root can be modified.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// let tempdir = TempDir::new("test", true);
    /// # let _tempdir = tempdir;
    /// # let tempdir = _tempdir.canonicalize().unwrap();
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// let root = dfs.new_root(tempdir, "test").unwrap();
    /// let connected_root = root.connect();
    ///
    /// connected_root.unwrap();
    /// ```
    pub fn connect(self) -> Result<ConnectedRoot<'dfs, GS, Sled>, DbConnectionError<<Sled as LocalStore>::Error>> {
        self.connect_with::<Sled>()
    }

    /// Usually you will want to connect to a Heed [`LocalStore`] as this is the main
    /// (and currently only) supported store type. Use [`connect`] for this.
    pub fn connect_with<LS: LocalStore>(self) -> Result<ConnectedRoot<'dfs, GS, LS>, DbConnectionError<LS::Error>> {
        ConnectedRoot::new(self)
    }
}


pub struct ConnectedRoot<'dfs, GS, LS = Heed> {
    root: Root<'dfs, GS>,
    pub(crate) connection: LS,
}

impl<'dfs, GS, LS> Deref for ConnectedRoot<'dfs, GS, LS> {
    type Target = Root<'dfs, GS>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

impl<'dfs, GS, LS> DerefMut for ConnectedRoot<'dfs, GS, LS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.root
    }
}

impl<'dfs, GS: GlobalStore, LS: LocalStore> ConnectedRoot<'dfs, GS, LS> {
    /// Refer to [`Root::connect`]
    pub(crate) fn new(root: Root<'dfs, GS>) -> Result<Self, DbConnectionError<LS::Error>> {
        let mut db_path = root.path().clone();

        if !db_path.exists() {
            return Err(DbConnectionError::RootPathDoesntExist(db_path));
        }

        if !db_path.is_dir() {
            return Err(DbConnectionError::PathIsFile(db_path));
        }

        db_path.push(&root.dfs.cfg().local_db);

        if !db_path.exists() {
            // the root exists but the .dfs folder does not
            create_dir_all(&db_path).map_err(|e| DbConnectionError::CreateFolder(db_path.clone(), e))?;
        }

        // now it must exist!
        assert!(db_path.exists());

        let connection = LS::new(&db_path)?;

        Ok(Self {
            root,
            connection,
        })
    }

    /// Index the root. This recursively goes through all subfolders of the root
    /// and adds an entry for each in the [`LocalStore`].
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() {
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use dfs::test::populated_tempdir;
    /// // create a tempdir with some test files in it
    /// let tempdir = populated_tempdir("test");
    /// let cfg = Config::test_config(&tempdir);
    /// let dfs = Dfs::new(cfg).unwrap();
    /// let root = dfs.new_root(&tempdir, "test").unwrap();
    /// let mut connected_root = root.connect().unwrap();
    ///
    /// // Do the indexing
    /// assert!(connected_root.index().await.is_ok());
    /// # }
    /// ```
    pub async fn index(&'dfs mut self) -> Result<(), IndexError<LS::Error>> {
        let indexer = Indexer::new(self)?;
        indexer.index().await?;

        Ok(())
    }

    /// Get the [`DirEntry`] of the topmost of this root. the path of this [`DirEntry`]
    /// is `/`. Using [`children`], other entries can be looked up from this root.
    ///
    /// On a brand new root (just created with [`new_root`]), the root direntry may not
    /// exist yet. This method will first create it in the [`LocalStore`] and then return it.
    ///
    /// TODO: make children method on DirEntry
    pub fn root_dir(&self) -> Result<DirEntry<GS, LS>, GetRootEntryError<LS::Error>> {
        if !self.path.exists() {
            return Err(GetRootEntryError::Exists(self.path.clone()))
        }

        if let Some(id) = self.root_direntry_id {
            if let Some(entry) = self.connection.get_direntry(id)? {
                Ok(DirEntry::from_storable(self, entry))
            } else {
                self.create_root()
            }
        } else {
            self.create_root()
        }
    }

    #[doc(hidden)]
    fn create_root(&self) -> Result<DirEntry<GS, LS>, GetRootEntryError<LS::Error>> {
        if !self.path.is_dir() {
            return Err(GetRootEntryError::NotDir(self.path.clone()))
        }

        let root = DirEntry::new(self, "/".into(), None, true);

        let _ = self.connection.put_direntry(root.id(), root.deref(), true)?;

        Ok(root)
    }

    /// Get a [`DirEntry`] by it's uuid.
    pub fn get_by_id(&self, id: Uuid) -> Result<Option<DirEntry<'_, 'dfs, GS, LS>>, GetDirEntryError<LS::Error>> {
        Ok(
            self.connection.get_direntry(id)?
            .map(|entry| DirEntry::from_storable(self, entry))
        )
    }
}


#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use std::ops::Deref;
    use std::path::PathBuf;

    use temp_testdir::TempDir;

    use crate::config::Config;
    use crate::Dfs;

    #[test]
    fn connect() {
        let root_a_dir = TempDir::new("test a", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        let dfs = Dfs::new(cfg.clone()).unwrap();

        let root_a = dfs.new_root(&root_a_dir, "a").unwrap();

        root_a.connect().unwrap();
    }

    #[test]
    fn get_root() {
        let root_a_dir = TempDir::new("test a", true);
        let mut actual_root = root_a_dir.deref().to_path_buf();

        let name: PathBuf = "test".into();
        actual_root.push(&name);
        create_dir_all(&actual_root).unwrap();

        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        let dfs = Dfs::new(cfg.clone()).unwrap();

        let root_a = dfs.new_root(&actual_root, "a").unwrap();

        let connected_a = root_a.connect().unwrap();
        let root_dir = connected_a.root_dir().unwrap();

        assert_eq!(root_dir.path(), PathBuf::from("/"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn large_index() {
        env_logger::builder().filter_level(log::LevelFilter::Info).init();

        let t = TempDir::new("test", true);
        let cfg = Config::test_config(&t);

        let dfs = Dfs::new(cfg).unwrap();

        let root_a = dfs.new_root("/home/jonathan/.config", "a").unwrap();

        let mut connected_a = root_a.connect().unwrap();

        connected_a.index().await.unwrap()
    }
}
