use std::path::Path;

use heed::{Database, Env, EnvOpenOptions};
use heed::types::SerdeBincode;
use uuid::Uuid;

use crate::global_store::PutStatus;
use crate::root::local_store::LocalStore;
use crate::root::dir_entry::StorableDirEntry;

pub struct Heed {
    env: Env,
    direntries: Database<SerdeBincode<Uuid>, SerdeBincode<StorableDirEntry>>,
}

impl LocalStore for Heed {
    type Error = heed::Error;

    fn new(path: &Path) -> Result<Self, Self::Error> {
        let env = EnvOpenOptions::new()
            .max_dbs(3)
            .map_size(2 * 1024 * 1024 * 1024)
            .open(path)?;


        Ok(Self {
            direntries: env.create_database(Some("direntries"))?,
            env,
        })
    }

    fn put_direntry(&self, id: Uuid, dir: &StorableDirEntry, overwrite: bool) -> Result<PutStatus, Self::Error> {
        let mut txn = self.env.write_txn()?;

        // if !overwrite && (self.direntries.get(&txn, &id)?.is_some()) {
        //     return Ok(PutStatus::Exists)
        // }

        self.direntries.put(&mut txn, &id, dir)?;

        txn.commit()?;

        Ok(PutStatus::Ok)
    }

    fn get_direntry(&self, id: Uuid) -> Result<Option<StorableDirEntry>, Self::Error> {
        let txn = self.env.read_txn()?;
        let res = self.direntries.get(&txn, &id)?;
        Ok(res)
    }
}
