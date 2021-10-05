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

#[derive(Serialize, Deserialize)]
pub struct StorableDirEntry {
    /// the name of this entry. This name is a relative path to the dfs root
    name: PathBuf,

    /// is this a dir or a file?
    entry_type: DirEntryType,

    /// the id of this entry
    uuid: Uuid,

    /// optional id of the parent of this entry
    parent: Option<Uuid>,
}

impl StorableDirEntry {
    pub fn is_dir(&self) -> bool {
        matches!(self.entry_type, DirEntryType::Dir{..})
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn name(&self) -> &Path {
        self.name.as_path()
    }

    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

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

    pub fn new(root: &'root ConnectedRoot<'dfs, GS, LS>, name: PathBuf, parent: Option<Uuid>, is_dir: bool) -> Self {
        let uuid = Uuid::new_v4();

        Self::from_storable(
            root,
            StorableDirEntry {
                name,
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

