use uuid::Uuid;

use crate::peer::Peer;
use crate::root::StorableRoot;
use std::path::Path;

pub mod heed_store;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PutStatus {
    Ok,
    Exists
}

impl PutStatus {
    pub fn to_err<E>(&self, f: impl FnOnce() -> E) -> Result<(), E> {
        if self == &PutStatus::Exists {
            Err(f())
        } else {
            Ok(())
        }
    }
}

impl PutStatus {
    pub fn exists(&self) -> bool {
        matches!(self, PutStatus::Exists)
    }
}

/// The GlobalStore is a store used by a DFS. All Roots in this DFS share this store.
/// The GlobalStore stores information about peers and roots.
///
/// TODO: GlobalStore systemwide or per-user?
/// TODO: Do we allow overlapping roots (maybe from different users)
pub trait GlobalStore: Sized + Sync {
    type Error;

    fn new(path: &Path) -> Result<Self, Self::Error>;

    fn put_peer(&self, id: Uuid, peer: &Peer, overwrite: bool) -> Result<PutStatus, Self::Error>;
    fn get_peer(&self, id: Uuid) -> Result<Option<Peer>, Self::Error>;
    fn get_all_peers(&self) -> Result<Vec<Peer>, Self::Error>;

    fn put_root(&self, id: Uuid, root: &StorableRoot, overwrite: bool) -> Result<PutStatus, Self::Error>;
    fn get_root(&self, id: Uuid) -> Result<Option<StorableRoot>, Self::Error>;
    fn get_root_by_name(&self, name: &str) -> Result<Option<StorableRoot>, Self::Error>;
    fn get_all_roots(&self) -> Result<Vec<StorableRoot>, Self::Error>;
}