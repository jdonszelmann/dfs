use crate::config::Config;
use crate::root::{Root, RootPathDoesntExistError};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use thiserror::Error;
use std::io;
use crate::root;

#[derive(Debug, Error)]
pub enum NewDfsError {
    #[error("sqlite connection error: {0}")]
    Sqlite(#[from] rusqlite::Error)
}

#[derive(Debug, Error)]
pub enum GetRootError {
    #[error("failed to compile or run sql statement: {0}")]
    CompileStatement(#[from] rusqlite::Error),

    #[error("found that root was removed from disk backing while getting root struct from database: {:?}", (.0).0)]
    RemovedFromDisk(#[from] RootPathDoesntExistError),
}

#[derive(Debug, Error)]
pub enum NewRootError {
    #[error("failed to compile sql statement: {0}")]
    CompileStatement(#[from] rusqlite::Error),

    #[error("db interaction error: {0}")]
    DbInteractionError(#[from] GetRootError),

    #[error("failed to canonicalize path: {0}")]
    CanonicalizePath(#[from] io::Error),

    #[error("path wasn't properly encoded utf8")]
    Utf8,

    #[error("path for new root at {:?} doesn't point to an existing folder (please make it first)", (.0).0)]
    PathDoesntExist(#[from] root::RootPathDoesntExistError)
}


pub struct Dfs {
    cfg: Config,
    connection: Connection
}

impl Dfs {
    pub fn new(cfg: Config) -> Result<Self, NewDfsError> {
        Ok(Self {
            connection: connect_db(cfg.global_db.clone())?,
            cfg,
        })
    }

    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    pub fn new_root(&self, path: impl AsRef<Path>, name: impl AsRef<str>) -> Result<Root, NewRootError> {
        let path = path.as_ref().to_path_buf().canonicalize()?;

        let root = Root::new(self, path.clone(), name.as_ref().to_string())?;

        self.connection.execute(
            "INSERT INTO roots (path, name) VALUES (?1, ?2)",
            params![path.to_str().ok_or(NewRootError::Utf8)?, name.as_ref()],
        )?;

        Ok(root)
    }

    pub fn get_root(&self, name: impl AsRef<str>) -> Result<Root, GetRootError> {

        let root = self.connection.query_row(
            "select path, name from roots where name = ?1;",
            [name.as_ref()],
            |row| {
                let path: String = row.get(0)?;
                Ok(Root::new(self, path.into(), row.get(1)?))
            }
        )?;

        Ok(root?)
    }

    pub fn get_roots(&self) -> Result<Vec<Root>, GetRootError> {
        let mut res = self.connection.prepare("
            select path, name from roots;
        ")?;

        let roots = res.query_map([], |row| {
            let path: String = row.get(0)?;
            Ok(Root::new(self, path.into(), row.get(1)?))
        })?;

        Ok(roots.flatten().collect::<Result<_, _>>()?)
    }
}

pub(crate) fn connect_db(db_path: Option<PathBuf>) -> Result<Connection, NewDfsError>{
    let conn = if let Some(mut db_path) = db_path {
        db_path.push("dfs.db");
        Connection::open(db_path)?
    } else {
        Connection::open_in_memory()?
    };

    conn.execute("
         create table if not exists roots (
            id integer primary key unique,
            path text not null unique,
            name text not null unique
         )", [],
    )?;

    conn.execute("
         create table if not exists peers (
            id integer primary key unique,

            name text not null unique,
            last_known_ip text,

            public_key text not null,
            private_key text not null
         )", [],
    )?;

    conn.execute("
         create table if not exists peers_roots_join (
            id integer primary key unique,
            root_id integer,
            peer_id integer
         )", [],
    )?;

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use temp_testdir::TempDir;
    use crate::config::Config;
    use crate::Dfs;

    #[test]
    fn root_same_path() {
        let root_a_dir = TempDir::new("test a", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            assert!(dfs.new_root(root_a_dir, "a").is_err());
        }
    }

    #[test]
    fn root_same_name() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_b = dfs.new_root(&root_a_dir, "a").unwrap();
            assert!(dfs.new_root(root_b_dir, "a").is_err());
        }
    }

    #[test]
    fn root_different_name() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            let _root_b = dfs.new_root(&root_b_dir, "b").unwrap();
        }
    }

    #[test]
    fn persistent_root() {
        let root_a_dir = TempDir::new("test a", true);
        let root_b_dir = TempDir::new("test b", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();


            assert!(dfs.get_root("a").is_err());

            let _root_a = dfs.new_root(&root_a_dir, "a").unwrap();
            let _root_b = dfs.new_root(&root_b_dir, "b").unwrap();
        }

        {
            let dfs = Dfs::new(cfg.clone()).unwrap();
            let _a = dfs.get_root("a").unwrap();
            let _b = dfs.get_root("b").unwrap();
        }
    }
}