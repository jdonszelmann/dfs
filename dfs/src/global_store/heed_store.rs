use std::path::Path;

use heed::{Database, Env, EnvOpenOptions};
use heed::types::SerdeBincode;
use uuid::Uuid;

use crate::global_store::{GlobalStore, PutStatus};
use crate::peer::Peer;
use crate::root::StorableRoot;

/// GlobalStore implementation using the Heed key-value store.
pub struct Heed {
    env: Env,
    peers: Database<SerdeBincode<Uuid>, SerdeBincode<Peer>>,
    roots: Database<SerdeBincode<Uuid>, SerdeBincode<StorableRoot>>,
    root_names: Database<SerdeBincode<String>, SerdeBincode<Uuid>>,
}

impl GlobalStore for Heed {
    type Error = heed::Error;

    /// Create a new Heed store
    ///
    /// ```
    /// # use temp_testdir::TempDir;
    /// # use dfs::global_store::heed_store::Heed;
    /// use dfs::global_store::GlobalStore;
    ///
    /// let tempdir = TempDir::new("test", true);
    /// assert!(Heed::new(&tempdir).is_ok());
    /// ```
    fn new(path: &Path) -> Result<Self, Self::Error> {
        let env = EnvOpenOptions::new()
            .max_dbs(3)
            .open(path)?;


        Ok(Self {
            peers: env.create_database(Some("peers"))?,
            roots: env.create_database(Some("roots"))?,
            root_names: env.create_database(Some("roots_names"))?,
            env,
        })
    }

    /// Create a new Heed store
    ///
    /// ```
    /// # use temp_testdir::TempDir;
    /// # use dfs::global_store::heed_store::Heed;
    /// # use dfs::peer::Peer;
    /// use dfs::global_store::GlobalStore;
    ///
    /// let tempdir = TempDir::new("test", true);
    /// let store = Heed::new(&tempdir).unwrap();
    ///
    /// let peer = Peer::new("jonathan".to_string());
    ///
    /// assert!(store.put_peer(peer.id(), &peer, false).is_ok());
    /// ```
    ///
    /// If the overwrite variable is true, [`PutStatus`] can only ever be Ok.
    /// If overwrite is false, the function is not allowed to overwrite existing entries which already
    /// have the same uuid as the current peer. In that case, [`PutStatus`] is [`Exists`]
    ///
    /// ```
    /// # use temp_testdir::TempDir;
    /// # use dfs::global_store::heed_store::Heed;
    /// # use dfs::global_store::GlobalStore;
    /// # use dfs::peer::Peer;
    /// use dfs::global_store::PutStatus;
    ///
    /// # let tempdir = TempDir::new("test", true);
    /// # let store = Heed::new(&tempdir).unwrap();
    ///
    /// let peer = Peer::new("jonathan".to_string());
    /// assert_eq!(store.put_peer(peer.id(), &peer, false).unwrap(), PutStatus::Ok);
    /// assert_eq!(store.put_peer(peer.id(), &peer, false).unwrap(), PutStatus::Exists);
    /// assert_eq!(store.put_peer(peer.id(), &peer, true).unwrap(), PutStatus::Ok);
    /// ```
    ///
    fn put_peer(&self, id: Uuid, peer: &Peer, overwrite: bool) -> Result<PutStatus, Self::Error> {

        let mut txn = self.env.write_txn()?;

        if !overwrite && self.peers.get(&txn, &id)?.is_some() {
            return Ok(PutStatus::Exists)
        }

        self.peers.put(&mut txn, &id, peer)?;
        txn.commit()?;

        Ok(PutStatus::Ok)
    }

    fn get_peer(&self, id: Uuid) -> Result<Option<Peer>, Self::Error> {
        let txn = self.env.read_txn()?;
        let res = self.peers.get(&txn, &id)?;
        Ok(res)
    }

    fn get_all_peers(&self) -> Result<Vec<Peer>, Self::Error> {
        let txn = self.env.read_txn()?;

        let peers = self.peers.iter(&txn)?
            .map(|i| i.map(|i| i.1))
            .collect::<Result<_, _>>()?;
        Ok(peers)
    }

    fn put_root(&self, id: Uuid, root: &StorableRoot, overwrite: bool) -> Result<PutStatus, Self::Error> {
        let mut txn = self.env.write_txn()?;

        if !overwrite && (
            self.roots.get(&txn, &id)?.is_some()
                || self.root_names.get(&txn, &root.name().to_string())?.is_some()
        ){
            return Ok(PutStatus::Exists)
        }

        self.roots.put(&mut txn, &id, root)?;
        self.root_names.put(&mut txn, &root.name().to_string(), &id)?;

        txn.commit()?;

        Ok(PutStatus::Ok)
    }

    fn get_root(&self, id: Uuid) -> Result<Option<StorableRoot>, Self::Error> {
        let txn = self.env.read_txn()?;
        let res = self.roots.get(&txn, &id)?;
        Ok(res)
    }

    fn get_root_by_name(&self, name: &str) -> Result<Option<StorableRoot>, Self::Error> {
        let txn = self.env.read_txn()?;
        if let Some(id) = self.root_names.get(&txn, &name.to_string())? {
            Ok(self.roots.get(&txn, &id)?)
        } else {
            Ok(None)
        }
    }

    fn get_all_roots(&self) -> Result<Vec<StorableRoot>, Self::Error> {
        let txn = self.env.read_txn()?;

        let roots = self.roots.iter(&txn)?
            .map(|i| i.map(|i| i.1))
            .collect::<Result<_, _>>()?;
        Ok(roots)
    }
}
