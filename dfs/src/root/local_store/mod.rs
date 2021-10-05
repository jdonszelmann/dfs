
use uuid::Uuid;

use std::path::Path;
use crate::global_store::PutStatus;
use crate::root::dir_entry::StorableDirEntry;

pub mod heed_store;

pub trait LocalStore: Sized + 'static {
    type Error;

    /// Creat a new database connection.
    ///
    /// If path is None, returns an in-memory database
    fn new(path: &Path) -> Result<Self, Self::Error>;

    fn put_direntry(&self, id: Uuid, dir: &StorableDirEntry, overwrite: bool) -> Result<PutStatus, Self::Error>;
    fn get_direntry(&self, id: Uuid) -> Result<Option<StorableDirEntry>, Self::Error>;
}

