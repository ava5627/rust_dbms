#![allow(dead_code)]
#![allow(unused_variables)]
use crate::{constants::DataType, index_file::IndexFile, record::Record};

struct Column {
    name: String,
    data_type: DataType,
    nullable: bool,
}

pub struct Table {
    name: String,
    columns: Vec<Column>,
    path: String,
}

impl Table {
    fn new(name: String, columns: Vec<Column>, path: String) -> Table {
        Table {
            name,
            columns,
            path,
        }
    }

    fn load_table(&self, table_name: String) -> Table {
        todo!()
    }

    fn search(&self, column_name: String, value: DataType, operator: String) -> Vec<Record> {
        todo!()
    }

    fn get_index_file(&self, column_name: String) -> IndexFile {
        todo!()
    }

    fn get_column_type(&self, column_name: String) -> DataType {
        todo!()
    }

    fn insert(&self, values: Vec<DataType>) -> bool {
        todo!()
    }

    fn delete(&self, column_name: String, value: DataType, operator: String) -> i32 {
        todo!()
    }

    fn update(
        &self,
        search_column: String,
        search_value: DataType,
        search_operator: String,
        update_column: String,
        update_value: DataType,
    ) -> i32 {
        todo!()
    }

    fn drop_table(&self) -> bool {
        todo!()
    }

    fn index_exists(&self, column_name: String) -> bool {
        todo!()
    }

    fn create_index(&self, column_name: String) -> bool {
        todo!()
    }
}
