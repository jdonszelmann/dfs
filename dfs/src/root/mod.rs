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

#[derive(Serialize, Deserialize)]
pub struct StorableRoot {
    uuid: Uuid,
    path: PathBuf,
    name: String,
    root_direntry_id: Option<Uuid>,
}

impl StorableRoot {
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Uuid {
        self.uuid
    }
}

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
    pub fn from_storable(dfs: &'dfs Dfs<GS>, storable: StorableRoot) -> Self {
        Self {
            dfs,
            storable,
        }
    }

    pub fn new(dfs: &'dfs Dfs<GS>, name: String, path: PathBuf) -> Self {
        let uuid = Uuid::new_v4();

        Self::from_storable(dfs, StorableRoot {
            uuid,
            name,
            path,
            root_direntry_id: None
        })
    }

    pub fn connect(self) -> Result<ConnectedRoot<'dfs, GS, Heed>, DbConnectionError<<Heed as LocalStore>::Error>> {
        self.connect_with::<Heed>()
    }

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

    pub async fn index(&'dfs mut self) -> Result<(), IndexError<LS::Error>> {
        let indexer = Indexer::new(self)?;
        indexer.index().await?;

        Ok(())
    }

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

    fn create_root(&self) -> Result<DirEntry<GS, LS>, GetRootEntryError<LS::Error>> {
        if !self.path.is_dir() {
            return Err(GetRootEntryError::NotDir(self.path.clone()))
        }

        let root = DirEntry::new(self, "/".into(), None, true);

        let _ = self.connection.put_direntry(root.id(), root.deref(), true)?;

        Ok(root)
    }

    pub(crate) fn get_by_id(&self, id: Uuid) -> Result<Option<DirEntry<'_, 'dfs, GS, LS>>, GetDirEntryError<LS::Error>> {
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
    use crate::test::populated_tempdir;

    #[test]
    fn connect() {
        let root_a_dir = TempDir::new("test a", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = global.as_ref().to_path_buf();

        let mut dfs = Dfs::new(cfg.clone()).unwrap();

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

        let mut dfs = Dfs::new(cfg.clone()).unwrap();

        let root_a = dfs.new_root(&actual_root, "a").unwrap();

        let connected_a = root_a.connect().unwrap();
        let root_dir = connected_a.root_dir().unwrap();

        assert_eq!(root_dir.name(), PathBuf::from("/"))
    }

    #[tokio::test]
    async fn index() {
        env_logger::builder().filter_level(log::LevelFilter::Trace).init();

        let t = populated_tempdir("test");
        let cfg = Config::test_config(&t);

        let mut dfs = Dfs::new(cfg).unwrap();

        let root_a = dfs.new_root(&t, "a").unwrap();

        let mut connected_a = root_a.connect().unwrap();

        connected_a.index().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn large_index() {
        env_logger::builder().filter_level(log::LevelFilter::Info).init();

        let t = TempDir::new("test", true);
        let cfg = Config::test_config(&t);

        let mut dfs = Dfs::new(cfg).unwrap();

        let root_a = dfs.new_root("/home/jonathan/.config", "a").unwrap();

        let mut connected_a = root_a.connect().unwrap();

        connected_a.index().await.unwrap()
    }
}
