use std::path::PathBuf;
use std::u8;

use anyhow::Context;

use crate::data::{get_max, get_min};
use crate::metadata::{ColumnMetaData, TableMetaData};
use crate::{get_dtype, DValue, DType};
use anyhow::{anyhow, Result};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::ErrorKind;
type IndexSize = u64;
use lz4_flex;

const ROWS_PER_BLOCK: usize = 1024;


#[derive(PartialEq, Debug)]

enum IndexValue {
    Uint64(u64),
    String([u8; 8])
}

impl IndexValue {
    fn from_dvalue(value: &DValue) -> IndexValue {
        match value {
            DValue::String(s) => {
                let mut bytes = [0; 8];
                // use the first 8 bytes of the string
                let s_bytes = &s.as_bytes();
                let n_to_copy = std::cmp::min(s_bytes.len(), bytes.len());
                bytes[0..n_to_copy].copy_from_slice(&s_bytes[0..n_to_copy]);
                IndexValue::String(bytes)
            },
            DValue::Uint64(n) => IndexValue::Uint64(*n),
        }
    }

    fn write_bytes(&self, bytes: &mut [u8], ) ->() {
        match self {
            IndexValue::String(s) => {
                bytes.copy_from_slice(s);
            }
            IndexValue::Uint64(u) => {
                bytes.copy_from_slice(&u.to_be_bytes());
            }
        };
    }

    fn from_bytes(bytes: &[u8], dtype: DType) -> IndexValue {
        match dtype {
            DType::String => {
                let mut s_bytes = [0; 8];
                s_bytes.copy_from_slice(bytes);
                IndexValue::String(s_bytes)
            },
            DType::Uint64 => {
                let array: [u8; 8] = bytes.try_into().expect("Slice with incorrect length");
                let u = u64::from_be_bytes(array);
                IndexValue::Uint64(u)
            }
        }
    }
    
}



struct IndexEntry {
    start_position: IndexSize, // stored as 8 bytes big endian
    size: IndexSize, // stored as 8 bytes big endian. In reality you don't need this but it's much simpler if you have it.
    min: IndexValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
    max: IndexValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
}
impl IndexEntry {
    fn to_bytes(&self) -> [u8; 32]{
        let mut bytes = [0; 32];
        bytes[0..8].copy_from_slice(&self.start_position.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.size.to_be_bytes());
        self.min.write_bytes(&mut bytes[16..24]);
        self.max.write_bytes(&mut bytes[24..32]);
        bytes
    }
    fn from_bytes(bytes: &[u8; 32], dtype: DType) -> IndexEntry {
        let start_position = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let size = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let min = IndexValue::from_bytes(&bytes[16..24], dtype);
        let max = IndexValue::from_bytes(&bytes[24..32], dtype);
        IndexEntry {
            start_position,
            size,
            min,
            max,
        }
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_to_bytes() {
        let entry = IndexEntry {
            start_position: 1,
            size: 2,
            min: IndexValue::Uint64(3),
            max: IndexValue::from_dvalue(&DValue::String("longlonglong".to_string())),
        };
        let bytes = entry.to_bytes();
        let expected: [u8; 32] = [
            0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0, 3,
            b'l', b'o', b'n', b'g', b'l', b'o', b'n', b'g',
        ];
        assert_eq!(bytes, expected);
    }
    #[test]
    fn test_index_entry_from_dvalue() {
        assert_eq!(IndexValue::from_dvalue(&DValue::Uint64(42)), IndexValue::Uint64(42));
        assert_eq!(IndexValue::from_dvalue(&DValue::String("".to_string())), IndexValue::String([0, 0, 0, 0, 0 ,0 ,0 ,0]));
        assert_eq!(IndexValue::from_dvalue(&DValue::String("a".to_string())), IndexValue::String([b'a', 0, 0, 0, 0 ,0 ,0 ,0]));
        assert_eq!(IndexValue::from_dvalue(&DValue::String("longlonglong".to_string())), IndexValue::String([b'l', b'o', b'n', b'g', b'l', b'o', b'n', b'g',]));

    }
}

struct ColumnWriter<'a> {
    data_file: File,
    index_file: File,
    col: &'a ColumnMetaData,
    position: IndexSize,
}
fn create_writers<'a>(
    root_path: &PathBuf,
    table: &'a TableMetaData,
) -> Result<Vec<ColumnWriter<'a>>> {
    table
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

            let data_file_metadata = data_file
                .metadata()
                .with_context(|| "Couldn't get metadata")?;

            Ok(ColumnWriter {
                data_file,
                index_file,
                col,
                position: data_file_metadata.len(),
            })
        })
        .collect::<Result<Vec<ColumnWriter>>>()
}

struct ColumnReader<'a> {
    data_file: File,
    index_file: File,
    col: &'a ColumnMetaData,
    position: IndexSize,
}

fn create_readers<'a>(
    root_path: &PathBuf,
    table: &TableMetaData,
    columns: &'a Vec<ColumnMetaData>,
) -> Result<Vec<ColumnReader<'a>>> {
    columns
        .iter()
        .map(|col| {
            let data_file = OpenOptions::new()
                .write(false)
                .append(false)
                .create(false)
                .open(data_path(&root_path, &table.name, &col.name))
                .with_context(|| "Couldn't open data file")?;
            let index_file = OpenOptions::new()
                .write(false)
                .append(false)
                .create(false)
                .open(index_path(&root_path, &table.name, &col.name))
                .with_context(|| "Couldn't open index file")?;

            let data_file_metadata = data_file
                .metadata()
                .with_context(|| "Couldn't get metadata")?;

            Ok(ColumnReader {
                data_file,
                index_file,
                col,
                position: data_file_metadata.len(),
            })
        })
        .collect::<Result<Vec<ColumnReader>>>()
}

pub fn write_data(
    root_path: &PathBuf,
    table: &TableMetaData,
    data: &Vec<Vec<DValue>>,
) -> Result<()> {
    let mut writers = create_writers(root_path, table)?;

    for block in data.chunks(ROWS_PER_BLOCK) {
        struct BlockColumnState {
            min: DValue,
            max: DValue,
            buf: Vec<u8>,
        }
        let mut block_column_states: Vec<BlockColumnState> = block[0]
            .iter()
            .map(|val| BlockColumnState {
                min: val.clone(),
                max: val.clone(),
                buf: Vec::new(),
            })
            .collect();

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
            let col_state = &mut block_column_states[index];
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

            let index_entry = IndexEntry {
                start_position: writer.position,
                size: compressed_len as IndexSize,
                min: IndexValue::from_dvalue(&col_state.min),
                max: IndexValue::from_dvalue(&col_state.max),
            };
            let index_bytes = index_entry.to_bytes();

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

fn read_all(root_path: &PathBuf, table: &TableMetaData) -> Result<Vec<Vec<DType>>> {
    let mut rows = Vec::new();
    let mut readers = create_readers(root_path, table, &table.columns)?;
    let mut buffer = [0; 8];
    loop {
        match readers[0].index_file.read_exact(&mut buffer) {
            Ok(_) => {
                // Successfully read 8 bytes, process the data as required
                println!("{:?}", buffer);
            },
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                // Less than 8 bytes were left in the file or the file was empty
                // Handle this case if necessary, otherwise break out of the loop
                break;
            },
            Err(e) => return Err(e.into()), // Handle other errors
        }
    }

    return Ok(rows);
}

fn index_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.index", table_name, column_name))
}

fn data_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.data", table_name, column_name))
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

