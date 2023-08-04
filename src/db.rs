use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableMetaData {
    pub name: String,
    pub columns: Vec<ColumnMetaData>,
}

impl TableMetaData {
    pub fn new(name: &str, columns: Vec<ColumnMetaData>) -> TableMetaData {
        TableMetaData {
            name: name.to_string(),
            columns,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetaData {
    pub name: String,
    pub dtype: DType,
}

impl ColumnMetaData {
    pub fn new(name: &str, dtype: DType) -> ColumnMetaData {
        ColumnMetaData {
            name: name.to_string(),
            dtype,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DType {
    String,
    Uint64,
}

#[derive(Serialize, Deserialize, Debug)]

struct MetaData {
    tables: Vec<TableMetaData>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DB {
    pub path: PathBuf,
    pub tables: Vec<TableMetaData>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let meta_path = Self::meta_path(&path);
        let contents = fs::read_to_string(&meta_path).with_context(|| {
            format!(
                "Failed to load metadata from: {}",
                meta_path.to_string_lossy()
            )
        })?;

        let meta: MetaData = serde_json::from_str(&contents)?;

        Ok(DB {
            path: path.as_ref().to_path_buf(),
            tables: meta.tables,
        })
    }

    pub fn init<P: AsRef<Path>>(path: P, tables: Vec<TableMetaData>) -> Result<Self, io::Error> {
        let meta = MetaData {
            tables: tables.clone(),
        };
        let obj = json!(meta);
        let contents = serde_json::to_string_pretty(&obj).unwrap();

        fs::write(Self::meta_path(&path), contents)?;

        Ok(DB {
            path: path.as_ref().to_path_buf(),
            tables,
        })
    }

    pub fn write_data(&self) ->  Result<()> {


        // check that all the 

        Ok(())
    }

    fn meta_path<P: AsRef<Path>>(root_path: P) -> PathBuf {
        root_path.as_ref().join("meta.json")
    }
}
