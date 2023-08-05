#[cfg(test)]
mod tests {
    use ::function_name::named;
    use tempdir::TempDir;

    extern crate rtcdb;
    use rtcdb::{ColumnMetaData, DType, TableMetaData, DB, DValue};

    const TEST_TABLE_NAME: &str = "events";
    fn get_test_tables () -> Vec<TableMetaData> {
        vec![TableMetaData::new(
            TEST_TABLE_NAME,
            vec![
                ColumnMetaData::new("event", DType::String),
                ColumnMetaData::new("timestamp", DType::Uint64),
                ColumnMetaData::new("id", DType::Uint64),
            ],
        )]
    }

    #[test]
    #[named]
    fn test_init() {
        let tables = get_test_tables();
        let tmp_dir = TempDir::new(function_name!()).unwrap();
        let db_init = DB::init(tmp_dir.path(), tables.clone()).unwrap();

        assert_eq!(db_init.tables, tables);
    }


    #[test]
    #[named]
    fn test_open() {
        let tmp_dir = TempDir::new(function_name!()).unwrap();
        let db_init = DB::init(tmp_dir.path(), get_test_tables()).unwrap();
        let db_open = DB::open(tmp_dir.path()).unwrap();

        assert_eq!(db_open.tables, db_init.tables);
    }

    #[test]
    #[named]
    fn test_write() {
        let tmp_dir = TempDir::new(function_name!()).unwrap();
        let db = DB::init(tmp_dir.path(), get_test_tables()).unwrap();

        db.write_data(&TEST_TABLE_NAME.to_string(), &vec![
            vec![
                DValue::String("test".to_string()),
                DValue::Uint64(123),
                DValue::Uint64(456),
            ],
            vec![
                DValue::String("test2".to_string()),
                DValue::Uint64(1234),
                DValue::Uint64(4567),
            ],
        ]).unwrap()

    }
}
