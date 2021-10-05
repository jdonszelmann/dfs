use std::path::Path;

use uuid::Uuid;

use crate::global_store::PutStatus;
use crate::root::local_store::LocalStore;
use crate::root::dir_entry::StorableDirEntry;
use sled::{Db, Tree};
use thiserror::Error;

pub struct Sled {
    db: Db,
    direntries: Tree,
}

#[derive(Debug, Error)]
pub enum SledError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("sled transaction error: {0}")]
    Transaction(#[from] sled::transaction::TransactionError),

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

impl LocalStore for Sled {
    type Error = SledError;

    fn new(path: &Path) -> Result<Self, Self::Error> {
        let db = sled::open(path)?;

        Ok(Self {
            direntries: db.open_tree(b"direntries")?,
            db,
        })
    }

    fn put_direntry(&self, id: Uuid, dir: &StorableDirEntry, overwrite: bool) -> Result<PutStatus, Self::Error> {
        let s_id = bincode::serialize(&id)?;
        let s_dir = bincode::serialize(&dir)?;


        self.direntries.insert(s_id.as_slice(), s_dir.as_slice())?;
        // self.direntries.transaction(move |tx| {
        //
        //     if !overwrite && (tx.get(&s_id)?.is_some()) {
        //         return Ok(PutStatus::Exists)
        //     }
        //
        //     tx.insert(s_id.as_slice(), s_dir.as_slice())?;
        //
        //     Ok(PutStatus::Ok)
        // }).map_err(Into::into)
        Ok(PutStatus::Ok)
    }

    fn get_direntry(&self, id: Uuid) -> Result<Option<StorableDirEntry>, Self::Error> {
        let s_id = bincode::serialize(&id)?;

        self.direntries.get(s_id)?
            .map(|i| bincode::deserialize(&i))
            .transpose()
            .map_err(Into::into)
    }
}
