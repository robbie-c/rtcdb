use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

use crate::DType;

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

#[derive(Serialize, Deserialize, Debug)]
pub struct MetaData {
    pub(crate) tables: Vec<TableMetaData>,
}

pub fn load_metadata_file<P: AsRef<Path>>(path: P) -> Result<MetaData>  {
    let meta_path = meta_path(&path);
    let contents = fs::read_to_string(&meta_path).with_context(|| {
        format!(
            "Failed to read file: {}",
            meta_path.to_string_lossy()
        )
    })?;

    let meta: MetaData = serde_json::from_str(&contents).with_context(|| {
        format!(
            "Failed to parse JSON file: {}",
            meta_path.to_string_lossy()
        )
    })?;
    return Ok(meta);
}

pub fn create_metadata_file<P: AsRef<Path>>(path: P, tables: &Vec<TableMetaData>) -> Result<MetaData> {
    let meta = MetaData {
        tables: tables.clone(),
    };
    let obj = json!(meta);
    let contents = serde_json::to_string_pretty(&obj).unwrap();

    fs::write(meta_path(&path), contents)?;
    return Ok(meta)
}


fn meta_path<P: AsRef<Path>>(root_path: P) -> PathBuf {
    root_path.as_ref().join("metadata.json")
}