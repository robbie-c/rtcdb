use std::path::PathBuf;
use std::u8;

use anyhow::Context;

use crate::metadata::{ColumnMetaData, DType, TableMetaData};
use anyhow::{anyhow, Result};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
type IndexSize = u64;
use lz4_flex;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DValue {
    String(String),
    Uint64(u64),
}

const ROWS_PER_BLOCK: usize = 1024;

pub fn write_data(
    root_path: &PathBuf,
    table: &TableMetaData,
    data: &Vec<Vec<DValue>>,
) -> Result<()> {
    struct ColumnWriter<'a> {
        data_file: File,
        index_file: File,
        col: &'a ColumnMetaData,
        position: IndexSize,
    }

    let mut writers: Vec<ColumnWriter<'_>> = table
        .columns
        .iter()
        .map(|col| {
            let data_file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(data_path(&root_path, &table.name, &col.name))
                .with_context(|| "Couldn't open data file")?;
            let index_file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(index_path(&root_path, &table.name, &col.name))
                .with_context(|| "Couldn't open index file")?;

            let data_file_metadata = data_file.metadata().with_context(|| "Couldn't get metadata")?;

            Ok(ColumnWriter {
                data_file,
                index_file,
                col,
                position: data_file_metadata.len()
            })
        })
        .collect::<Result<Vec<ColumnWriter>>>()?;

    for block in data.chunks(ROWS_PER_BLOCK) {
        struct BlockColumnState {
            min: DValue,
            max: DValue,
            buf: Vec<u8>,
        }
        let mut block_column_states: Vec<BlockColumnState> = block[0].iter().map(|val| {
            BlockColumnState { 
                min: val.clone(), 
                max: val.clone(),
                buf: Vec::new()
            }
        }).collect();


        for row in block {
            for (index, col) in row.iter().enumerate() {
                let col_writer = &writers[index];
                let col_state = &mut block_column_states[index];
                if get_dtype(&col) != col_writer.col.dtype {
                    return Err(anyhow!("Mismatched data type"));
                }
                write_dvalue_data(&mut col_state.buf, &col)?;
                col_state.min = get_min(&col_state.min, &col).clone();
                col_state.max = get_max(&col_state.max, &col).clone();
            }
        }

        for (index, writer) in writers.iter_mut().enumerate() {
            let col_state= &mut block_column_states[index];
            let buf = &mut col_state.buf;
            let buf_size = buf.len();

            // compress the data
            let prealloc_size = lz4_flex::block::get_maximum_output_size(buf_size);
            let mut compress_output = vec![0; prealloc_size];
            let compressed_len = lz4_flex::block::compress_into(&buf, &mut compress_output)
                .with_context(|| "Couldn't compress data")?;
            col_state.buf.clear();

            // write the compressed data
            writer
                .data_file
                .write_all(&compress_output)
                .with_context(|| "Couldn't write compressed data")?;

            // prepare the entry in the index file
            struct IndexEntry {
                start_position: IndexSize, // stored as 8 bytes big endian
                size: IndexSize, // stored as 8 bytes big endian. In reality you don't need this but it's much simpler if you have it.
                min: DValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
                max: DValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
            }
            let index_entry = IndexEntry {
                start_position: writer.position,
                size: compressed_len as IndexSize,
                min: col_state.min.clone(),
                max: col_state.max.clone(),
            };
            let mut index_bytes = Vec::new();

            index_bytes.extend_from_slice(&index_entry.start_position.to_be_bytes());
            index_bytes.extend_from_slice(&index_entry.size.to_be_bytes());
            write_dvalue_index(&mut index_bytes, &index_entry.min)?;
            write_dvalue_index(&mut index_bytes, &index_entry.max)?;

            // write the index entry
            writer
                .index_file
                .write_all(&index_bytes)
                .with_context(|| "Couldn't write index entry")?;

            // increment the positions
            writer.position += compressed_len as IndexSize;
        }
    }
    Ok(())
}

fn index_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.index", table_name, column_name))
}

fn data_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.data", table_name, column_name))
}

fn get_dtype(value: &DValue) -> DType {
    match value {
        DValue::String(_) => DType::String,
        DValue::Uint64(_) => DType::Uint64,
    }
}

fn get_min<'a>(min_val: &'a DValue, new_val: &'a DValue) -> &'a DValue {
    match (min_val, new_val) {
        (DValue::String(min_s), DValue::String(new_s)) => {
            if new_s < min_s {
                new_val
            } else {
                min_val
            }
        }
        (DValue::Uint64(min_u), DValue::Uint64(new_u)) => {
            if new_u < min_u {
                new_val
            } else {
                min_val
            }
        }
        _ => panic!("Mismatched types"),
    }
}

fn get_max<'a>(max_val: &'a DValue, new_val: &'a DValue) -> &'a DValue {
    match (max_val, new_val) {
        (DValue::String(max_s), DValue::String(new_s)) => {
            if new_s > max_s {
                new_val
            } else {
                max_val
            }
        }
        (DValue::Uint64(max_u), DValue::Uint64(new_u)) => {
            if new_u > max_u {
                new_val
            } else {
                max_val
            }
        }
        _ => panic!("Mismatched types"),
    }
}

fn write_dvalue_data(bytes: &mut Vec<u8>, value: &DValue) -> Result<()> {
    match value {
        DValue::String(s) => {
            let null_terminated = format!("{}\0", s);
            bytes.extend_from_slice(&null_terminated.as_bytes());
        }
        DValue::Uint64(u) => {
            bytes.extend_from_slice(&u.to_be_bytes());
        }
    };
    Ok(())
}

fn write_dvalue_index(bytes: &mut Vec<u8>, value: &DValue) -> Result<()> {
    match value {
        DValue::String(s) => {
            // only write the first 8 bytes of the string
            let null_terminated = &(s.to_owned() + &"\0".repeat(8))[0..8];
            bytes.extend_from_slice(&null_terminated.as_bytes()[0..8]);
        }
        DValue::Uint64(u) => {
            bytes.extend_from_slice(&u.to_be_bytes());
        }
    };
    Ok(())
}
