use std::path::PathBuf;
use std::{u8, io};

use anyhow::Context;
use anyhow::__private::kind::TraitKind;

use crate::data::{get_max, get_min};
use crate::metadata::{ColumnMetaData, TableMetaData};
use crate::{get_dtype, DValue, DType};
use anyhow::{anyhow, Result, Error};
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

    fn from_bytes(bytes: &[u8], dtype: &DType) -> IndexValue {
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
    compressed_size: IndexSize, // stored as 8 bytes big endian. In reality you don't need this but it's much simpler if you have it.
    decompressed_size: IndexSize, // stored as 8 bytes big endian
    min: IndexValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
    max: IndexValue, // stored as 8 bytes, either the u64 in big endian or the first 8 bytes of the string
}
impl IndexEntry {
    fn to_bytes(&self) -> [u8; 40]{
        let mut bytes = [0; 40];
        bytes[0..8].copy_from_slice(&self.start_position.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.compressed_size.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.decompressed_size.to_be_bytes());
        self.min.write_bytes(&mut bytes[24..32]);
        self.max.write_bytes(&mut bytes[32..40]);
        bytes
    }
    fn from_bytes(bytes: &[u8; 40], dtype: &DType) -> IndexEntry {
        let start_position = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let compressed_size = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let decompressed_size = u64::from_be_bytes(bytes[16..24].try_into().unwrap());

        let min = IndexValue::from_bytes(&bytes[24..32], dtype);
        let max = IndexValue::from_bytes(&bytes[32..40], dtype);
        IndexEntry {
            start_position,

            compressed_size,
            decompressed_size,
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
            compressed_size: 2,
            decompressed_size: 3,
            min: IndexValue::Uint64(4),
            max: IndexValue::from_dvalue(&DValue::String("longlonglong".to_string())),
        };
        let bytes = entry.to_bytes();
        let expected: [u8; 40] = [
            0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0, 3,
            0, 0, 0, 0, 0, 0, 0, 4,
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
                write_dvalue_data(&mut col_state.buf, &col);
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
                compressed_size: compressed_len as IndexSize,
                decompressed_size: buf_size as IndexSize,
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

fn read_all(root_path: &PathBuf, table: &TableMetaData) -> Result<Vec<Vec<DValue>>> {
    let mut rows = Vec::new();
    let mut readers = create_readers(root_path, table, &table.columns)?;
    loop {
        let read_results: Result<Vec<IndexEntry>> = readers.iter_mut().map(|reader| {
            let mut buffer = [0; 40];
            reader.index_file.read_exact(&mut buffer)?;
            let index_entry = IndexEntry::from_bytes(&buffer, &reader.col.dtype);
            Ok(index_entry)
        }).collect();
        match read_results {
            Ok(indexes) => {
                read_all_from_block_group(&mut rows, &mut readers, indexes)?;
            },
            Err(e) => {
                let error: Error = e.into();
                match error.downcast_ref::<io::Error>() {
                    Some(io_error) if io_error.kind() == ErrorKind::UnexpectedEof => {
                        // Reached the end of the one of the index files
                        break;
                    }
                    _ => return Err(error),
                }
            }
        }
    }

    return Ok(rows);
}

fn read_all_from_block_group(rows: &mut Vec<Vec<DValue>>, readers: &mut Vec<ColumnReader>, indexes: Vec<IndexEntry>) -> Result<()> {
    // map over the readers, get all the column data for that reader
    let block_group_rows = readers.iter_mut().enumerate().map(|(i, reader)| -> Result<()> {
        let index_entry = &indexes[i];
        // load the next block from the data file
        let mut buffer = vec![0; index_entry.compressed_size as usize];
        reader.data_file.seek(io::SeekFrom::Start(index_entry.start_position))?;
        reader.data_file.read_exact(&mut buffer)?;
        // decompress the data
        let mut decompress_output = vec![0; index_entry.decompressed_size as usize];
        lz4_flex::block::decompress_into(&buffer, &mut decompress_output)?;
        // convert the bytes to DValues
        let mut block_rows = Vec::new();
        let mut bytes = decompress_output.as_slice(); 
        while bytes.len() > 0 {
            let (n_bytes, dvalue) = read_dvalue_data(bytes, &reader.col.dtype);
            block_rows.push(dvalue);
            bytes = &bytes[n_bytes..];
        }
        Ok(())
    });
    Ok(())
}

fn index_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.index", table_name, column_name))
}

fn data_path(root_path: &PathBuf, table_name: &String, column_name: &String) -> PathBuf {
    root_path.join(format!("{}.{}.data", table_name, column_name))
}

fn write_dvalue_data(bytes: &mut Vec<u8>, value: &DValue) {
    match value {
        DValue::String(s) => {
            // use length prefixed format, with a u32 for string length
            if s.len() > u32::MAX as usize {
                panic!("String too long");
            }
            let length_bytes: [u8; 4] = (s.len() as u32).to_be_bytes();
            bytes.extend_from_slice(&length_bytes);
            bytes.extend_from_slice(s.as_bytes());
        }
        DValue::Uint64(u) => {
            bytes.extend_from_slice(&u.to_be_bytes());
        }
    };
}

fn read_dvalue_data(bytes: &[u8], dtype: &DType) -> (usize, DValue) {
    match dtype {
        DType::String => {
            let length_bytes: [u8; 4] = bytes[0..4].try_into().unwrap();
            let length = u32::from_be_bytes(length_bytes) as usize;
            let s_bytes = &bytes[4..(4 + length)];
            // I double checked the docs, and the inverse to as_bytes is *actually* from_utf8
            let s = String::from_utf8(s_bytes.to_vec()).unwrap();
            (4 + length, DValue::String(s))
        }
        DType::Uint64 => {
            let u_bytes: [u8; 8] = bytes[0..8].try_into().unwrap();
            let u = u64::from_be_bytes(u_bytes);
            (8, DValue::Uint64(u))
        }
    }
}

