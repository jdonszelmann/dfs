use rusqlite::{Connection, Row, params};
use std::path::PathBuf;
use thiserror::Error;
use crate::Dfs;
use crate::dir_entry::DirEntry;
use std::fs::create_dir_all;
use std::io;
use std::ops::{Deref, DerefMut};
use crate::index::{IndexError, Indexer};


#[derive(Debug, Error)]
pub enum DbConnectionError {

    #[error("sqlite connection error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("the folder this root is associated with does not exist (anymore). The folder was {0:?}")]
    RootPathDoesntExist(PathBuf),

    #[error("the path of this dfs root points to a file, not a folder ({0:?})")]
    PathIsFile(PathBuf),

    #[error("failed to create the .dfs folder in {0:?}: {1}")]
    CreateFolder(PathBuf, io::Error),
}

#[derive(Debug, Error)]
pub enum GetDirEntryError {
    #[error("sqlite connection error: {0}")]
    Sqlite(#[from] rusqlite::Error)
}

#[derive(Debug, Error)]
#[error("path for new root at {0} doesn't point to an existing folder (please make it first)")]
pub struct RootPathDoesntExistError(pub PathBuf);


#[derive(Debug, Error)]
pub enum GetRootEntryError {
    #[error("sqlite connection error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("couldn't get root direntry. the direntry did not exist before and an attempt to create it did not help")]
    CouldntCreateRootEntry,

    #[error("failed to canonicalize path: {0}")]
    CanonicalizePath(#[from] io::Error),

    #[error("path wasn't properly encoded utf8")]
    Utf8,
}

pub struct Root<'dfs> {
    path: PathBuf,
    name: String,

    dfs: &'dfs Dfs,
}


impl<'dfs> Root<'dfs> {
    pub fn new(dfs: &'dfs Dfs, path: PathBuf, name: String) -> Result<Self, RootPathDoesntExistError> {

        if !path.exists() {
            return Err(RootPathDoesntExistError(path))
        }

        Ok(Self {
            path,
            name,
            dfs
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn connect(self) -> Result<ConnectedRoot<'dfs>, DbConnectionError> {
       ConnectedRoot::new(self)
    }
}

pub struct ConnectedRoot<'dfs> {
    root: Root<'dfs>,
    pub(crate) connection: Connection,
}

impl<'dfs> Deref for ConnectedRoot<'dfs> {
    type Target = Root<'dfs>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

impl<'dfs> DerefMut for ConnectedRoot<'dfs> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.root
    }
}

impl<'dfs> ConnectedRoot<'dfs> {
    pub(crate) fn new(root: Root<'dfs>) -> Result<Self, DbConnectionError> {
        let db_path = if let Some(ref local_db) = root.dfs.cfg().local_db {
            let mut db_path = root.path.clone();

            if !db_path.exists() {
                return Err(DbConnectionError::RootPathDoesntExist(db_path));
            }

            if !db_path.is_dir() {
                return Err(DbConnectionError::PathIsFile(db_path));
            }

            db_path.push(local_db);

            if !db_path.exists() {
                // the root exists but the .dfs folder does not
                create_dir_all(&db_path).map_err(|e| DbConnectionError::CreateFolder(db_path.clone(), e))?;
            }

            // now it must exist!
            assert!(db_path.exists());

            Some(db_path)
        } else {
            None
        };

        Ok(Self {
            root,
            connection: connect_db(db_path)?,
        })
    }

    pub async fn index(&mut self) -> Result<(), IndexError> {
        let indexer = Indexer::new(self)?;
        indexer.index().await?;

        Ok(())
    }

    pub fn root_dir<'root>(&'root self) -> Result<DirEntry<'root, 'dfs>, GetRootEntryError> {
        let mut tries = 0;
        loop {
            tries += 1;
            break match self.connection.query_row(
                "select * from files where parent is null;",
                [],
                |row| row_to_direntry(self, row)
            ) {
                Ok(i) => Ok(i),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    self.create_root()?;

                    if tries < 2 {
                        continue;
                    } else {
                        Err(GetRootEntryError::CouldntCreateRootEntry)
                    }
                },
                Err(e) => Err(e.into()),
            };
        }
    }

    fn create_root(&self) -> Result<(), GetRootEntryError> {
        let canonicalized = self.root.path.canonicalize()?;
        let name = canonicalized
            .file_name()
            .map(|i| i.to_str())
            .unwrap_or(Some("root"))
            .ok_or(GetRootEntryError::Utf8)?;

        self.connection.execute(
            "INSERT INTO files (name, parent, dir) VALUES (?1, ?2, ?3)",
            params![name, None::<u64>, 1],
        )?;

        Ok(())
    }

    pub(crate) fn get_by_id<'root>(&'root self, id: i64) -> Result<DirEntry<'root, 'dfs>, GetDirEntryError> {
        self.connection.query_row(
            "select * from files where parent = ?1;",
            [id],
            |row| row_to_direntry(self, row)
        ).map_err(Into::into)
    }
}


pub(crate) fn connect_db(db_path: Option<PathBuf>) -> Result<Connection, DbConnectionError>{
    let conn = if let Some(mut db_path) = db_path {
        db_path.push("dfs.db");
        Connection::open(db_path)?
    } else {
        Connection::open_in_memory()?
    };

    conn.execute("
         create table if not exists files (
            id integer primary key unique,
            name text not null,
            parent integer ,
            dir integer not null,

            foreign key (parent) references files (id) on delete cascade
         )",
        [],
    )?;

    Ok(conn)
}

fn row_to_direntry<'root, 'dfs>(root: &'root ConnectedRoot<'dfs>, row: &'_ Row) -> Result<DirEntry<'root, 'dfs>, rusqlite::Error> {
    let name: String = row.get(row.column_index("name")?)?;
    Ok(DirEntry::new(
        root,
        row.get(row.column_index("id")?)?,
        name.into(),
        row.get(row.column_index("parent")?)?,
        row.get(row.column_index("dir")?)?,
    ))
}

#[cfg(test)]
mod tests {
    use temp_testdir::TempDir;
    use crate::config::Config;
    use crate::Dfs;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::fs::create_dir_all;
    use crate::test::populated_tempdir;

    #[test]
    fn connect() {
        let root_a_dir = TempDir::new("test a", true);
        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        let dfs = Dfs::new(cfg.clone()).unwrap();

        let root_a = dfs.new_root(&root_a_dir, "a").unwrap();

        root_a.connect().unwrap();
    }

    #[test]
    fn get_root() {
        let root_a_dir = TempDir::new("test a", true);
        let mut actual_root = root_a_dir.deref().to_path_buf();

        let name: PathBuf = "test".into();
        actual_root.push(&name);
        create_dir_all(&actual_root).unwrap();

        let global = TempDir::new("global", true);

        let mut cfg = Config::default();
        cfg.global_db = Some(global.as_ref().to_path_buf());

        let dfs = Dfs::new(cfg.clone()).unwrap();

        let root_a = dfs.new_root(&actual_root, "a").unwrap();

        let connected_a = root_a.connect().unwrap();
        let root_dir = connected_a.root_dir().unwrap();

        assert_eq!(root_dir.name(), name)
    }

    #[tokio::test]
    async fn index() {
        env_logger::builder().filter_level(log::LevelFilter::Trace).init();

        let t = populated_tempdir("test");

        let cfg = Config::test_config();

        let dfs = Dfs::new(cfg).unwrap();

        let root_a = dfs.new_root(&t, "a").unwrap();

        let mut connected_a = root_a.connect().unwrap();

        connected_a.index().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn large_index() {
        env_logger::builder().filter_level(log::LevelFilter::Info).init();

        let mut cfg = Config::test_config();
        cfg.local_db = None;

        let dfs = Dfs::new(cfg).unwrap();

        let root_a = dfs.new_root("/home/jonathan/.config", "a").unwrap();

        let mut connected_a = root_a.connect().unwrap();

        connected_a.index().await.unwrap()
    }
}