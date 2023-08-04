
use tempdir::TempDir;
use ::function_name::named;

extern crate rtcdb;
use rtcdb::db::{DB, TableMetaData, ColumnMetaData, DType};

#[test]
#[named]
fn test_setup() {
    let tmp_dir = TempDir::new(function_name!()).unwrap();
    let tables = vec![TableMetaData::new(
        "events",
         vec![
             ColumnMetaData::new (
                 "id",
                  DType::Uint64
             ),
             ColumnMetaData::new (
                 "timestamp",
                  DType::Uint64
             ),
             ColumnMetaData::new (
                 "event",
                  DType::String
             )
         ]
         )];
    DB::init(tmp_dir.path(), tables.clone()).unwrap();

    let db_open = DB::open(tmp_dir.path()).unwrap();

    assert_eq!(db_open.tables, tables);

}