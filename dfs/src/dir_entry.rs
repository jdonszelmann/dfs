use crate::root::{ConnectedRoot, GetDirEntryError};
use std::path::{PathBuf, Path};

pub enum DirEntryType {
    Dir,
    File
}

pub struct DirEntry<'root, 'dfs> {
    root: &'root ConnectedRoot<'dfs>,

    /// the name of this entry. This name is a relative path to the dfs root
    name: PathBuf,

    /// is this a dir or a file?
    entry_type: DirEntryType,

    /// the id of this entry
    id: i64,

    /// optional id of the parent of this entry
    parent: Option<i64>,
}


impl<'root, 'dfs> DirEntry<'root, 'dfs> {
    pub(crate) fn new(root: &'root ConnectedRoot<'dfs>, id: i64, name: PathBuf, parent: Option<i64>, is_dir: i64) -> Self {
        Self {
            root,
            name,
            entry_type: if is_dir == 1 {DirEntryType::Dir} else {DirEntryType::File},
            id,
            parent
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.entry_type, DirEntryType::Dir{..})
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn name(&self) -> &Path {
        self.name.as_path()
    }

    pub fn parent(&self) -> Option<Result<DirEntry<'root, 'dfs>, GetDirEntryError>> {
        Some(self.root.get_by_id(self.parent?))
    }

    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    pub(crate) fn id(&self) -> i64 {
        self.id
    }
}

