// use std::path::Path;
//
// use uuid::Uuid;
//
// use crate::global_store::PutStatus;
// use crate::root::local_store::LocalStore;
// use crate::root::dir_entry::StorableDirEntry;
// use thiserror::Error;
// use rusqlite::Connection;
//
// pub struct Sqlite {
//     connection: Connection,
// }
//
// impl LocalStore for Sqlite {
//     type Error = rusqlite::Error;
//
//     fn new(path: &Path) -> Result<Self, Self::Error> {
//         let connection = rusqlite::Connection::open(path)?;
//
//         connection.execute("
//             create table files (
//                 id integer primary key
//                 uuid string
//
//             )
//         ", [])?;
//
//         Ok(Self {
//             connection
//         })
//     }
//
//     fn put_direntry(&self, id: Uuid, dir: &StorableDirEntry, overwrite: bool) -> Result<PutStatus, Self::Error> {
//         let s_id = bincode::serialize(&id)?;
//         let s_dir = bincode::serialize(&dir)?;
//
//
//         self.direntries.insert(s_id.as_slice(), s_dir.as_slice())?;
//         // self.direntries.transaction(move |tx| {
//         //
//         //     if !overwrite && (tx.get(&s_id)?.is_some()) {
//         //         return Ok(PutStatus::Exists)
//         //     }
//         //
//         //     tx.insert(s_id.as_slice(), s_dir.as_slice())?;
//         //
//         //     Ok(PutStatus::Ok)
//         // }).map_err(Into::into)
//         Ok(PutStatus::Ok)
//     }
//
//     fn get_direntry(&self, id: Uuid) -> Result<Option<StorableDirEntry>, Self::Error> {
//         let s_id = bincode::serialize(&id)?;
//
//         self.direntries.get(s_id)?
//             .map(|i| bincode::deserialize(&i))
//             .transpose()
//             .map_err(Into::into)
//     }
// }
