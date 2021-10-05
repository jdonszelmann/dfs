use crate::root::{ConnectedRoot, GetDirEntryError};
use std::path::{PathBuf, Path};
use crate::global_store::GlobalStore;
use std::ops::{Deref, DerefMut};
use serde::{Serialize, Deserialize};
use crate::root::local_store::LocalStore;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub enum DirEntryType {
    Dir,
    File
}

/// Storable version of a [`DirEntry`]. For documentation refer to [`DirEntry`]
#[derive(Serialize, Deserialize)]
pub struct StorableDirEntry {
    /// the name of this entry. This name is a relative path to the dfs root
    path: PathBuf,

    /// is this a dir or a file?
    entry_type: DirEntryType,

    /// the id of this entry
    uuid: Uuid,

    /// optional id of the parent of this entry
    parent: Option<Uuid>,
}

impl StorableDirEntry {
    /// Returns whether or not this entry is a directory
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # use dfs::root::dir_entry::DirEntry;
    /// # let tempdir = TempDir::new("test", true);
    /// # let _tempdir = tempdir;
    /// # let tempdir = _tempdir.canonicalize().unwrap();
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// # let root = dfs.new_root(tempdir, "test").unwrap();
    /// # let connected_root = root.connect().unwrap();
    ///
    /// // Not the standard way to make DirEntries. Usually you use `index` on a root
    /// // to have it collect the entries for you.
    /// let entry = DirEntry::new(&connected_root, "/test".into(), None, true);
    /// assert!(entry.is_dir());
    /// ```
    pub fn is_dir(&self) -> bool {
        matches!(self.entry_type, DirEntryType::Dir{..})
    }

    /// Returns whether or not this entry is a file
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # use dfs::root::dir_entry::DirEntry;
    /// # let tempdir = TempDir::new("test", true);
    /// # let _tempdir = tempdir;
    /// # let tempdir = _tempdir.canonicalize().unwrap();
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// # let root = dfs.new_root(tempdir, "test").unwrap();
    /// # let connected_root = root.connect().unwrap();
    ///
    /// // Not the standard way to make DirEntries. Usually you use `index` on a root
    /// // to have it collect the entries for you.
    /// let entry = DirEntry::new(&connected_root, "/test".into(), None, false);
    /// assert!(entry.is_file());
    /// ```
    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    /// Returns whether or not this entry is a directory
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # use dfs::root::dir_entry::DirEntry;
    /// # use std::path::PathBuf;
    /// # use std::str::FromStr;
    /// # let tempdir = TempDir::new("test", true);
    /// # let _tempdir = tempdir;
    /// # let tempdir = _tempdir.canonicalize().unwrap();
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    ///
    /// # let root = dfs.new_root(tempdir, "test").unwrap();
    /// # let connected_root = root.connect().unwrap();
    ///
    /// // Not the standard way to make DirEntries. Usually you use `index` on a root
    /// // to have it collect the entries for you.
    /// let entry = DirEntry::new(&connected_root, "/test".into(), None, true);
    /// assert_eq!(entry.path(), &PathBuf::from("/test".to_string()));
    /// ```
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Return whether or not this direntry is the top level directory of the root.
    ///
    /// ```
    /// # use dfs::config::Config;
    /// # use dfs::Dfs;
    /// # use temp_testdir::TempDir;
    /// # use dfs::root::dir_entry::DirEntry;
    /// # use std::path::PathBuf;
    /// # use std::str::FromStr;
    /// # let tempdir = TempDir::new("test", true);
    /// # let _tempdir = tempdir;
    /// # let tempdir = _tempdir.canonicalize().unwrap();
    /// # let mut cfg = Config::default();
    /// # cfg.global_db = tempdir.to_path_buf();
    /// # let dfs = Dfs::new(cfg).unwrap();
    /// #
    /// # let root = dfs.new_root(tempdir, "test").unwrap();
    /// # let connected_root = root.connect().unwrap();
    ///
    /// // Not the standard way to make DirEntries. Usually you use `index` on a root
    /// // to have it collect the entries for you.
    /// let entry = DirEntry::new(&connected_root, "/test".into(), None, true);
    /// assert_eq!(entry.is_root());
    /// ```
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    #[doc(hidden)]
    pub(crate) fn id(&self) -> Uuid {
        self.uuid
    }
}

pub struct DirEntry<'root, 'dfs, GS, LS> {
    root: &'root ConnectedRoot<'dfs, GS, LS>,
    storable: StorableDirEntry,
}

impl<'root, 'dfs, GS, LS> DerefMut for DirEntry<'root, 'dfs, GS, LS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storable
    }
}

impl<'root, 'dfs, GS, LS> Deref for DirEntry<'root, 'dfs, GS, LS> {
    type Target = StorableDirEntry;

    fn deref(&self) -> &Self::Target {
        &self.storable
    }
}


impl<'root, 'dfs, GS: GlobalStore, LS: LocalStore> DirEntry<'root, 'dfs, GS, LS> {
    pub fn from_storable(root: &'root ConnectedRoot<'dfs, GS, LS>, storable: StorableDirEntry) -> Self {
        Self {
            root,
            storable
        }
    }

    pub fn new(root: &'root ConnectedRoot<'dfs, GS, LS>, path: PathBuf, parent: Option<Uuid>, is_dir: bool) -> Self {
        let uuid = Uuid::new_v4();

        Self::from_storable(
            root,
            StorableDirEntry {
                path,
                entry_type: if is_dir { DirEntryType::Dir } else { DirEntryType::File },
                uuid,
                parent
            }
        )
    }

    pub fn parent(&self) -> Result<Option<DirEntry<'root, 'dfs, GS, LS>>, GetDirEntryError<LS::Error>> {
        if let Some(parent) = self.parent {
            self.root.get_by_id(parent)
        } else {
            Ok(None)
        }
    }
}

