pub mod metadata;
pub mod storage;
pub mod data;

use anyhow::{Result, anyhow};
use metadata::create_metadata_file;
use std::path::{Path, PathBuf};
use metadata::load_metadata_file;
use storage::write_data;

pub use metadata::{ColumnMetaData, MetaData, TableMetaData};
pub use data::{DType, DValue, get_dtype};


#[derive(Debug, PartialEq, Eq)]
pub struct DB {
    pub path: PathBuf,
    pub tables: Vec<TableMetaData>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let meta = load_metadata_file(&path)?;

        Ok(DB {
            path: path.as_ref().to_path_buf(),
            tables: meta.tables,
        })
    }

    pub fn init<P: AsRef<Path>>(path: P, tables: Vec<TableMetaData>) -> Result<Self> {
        let meta = create_metadata_file(&path, &tables)?;

        Ok(DB {
            path: path.as_ref().to_path_buf(),
            tables: meta.tables,
        })
    }

    pub fn write_data(&self, table_name: &String, rows: &Vec<Vec<DValue>>) ->  Result<()> {
        // get table metadata
        let table = self.tables.iter().find(|table| {
            table.name.eq(table_name)
        }).ok_or(anyhow!("No table with name: {}", table_name))?;

        write_data(&self.path, &table, rows)?;

        Ok(())
    }


}
