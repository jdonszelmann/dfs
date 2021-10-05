use temp_testdir::TempDir;
use std::path::Path;

use std::{io, fs};

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

pub fn populated_tempdir(name: impl AsRef<Path>) -> TempDir {
    let t = TempDir::new(name, true);

    copy_dir_all("tests/fake_dir", &t).unwrap();

    t
}
