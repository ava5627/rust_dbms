use std::cmp::min;

use crate::{constants::DataType, index_file::IndexFile, record::Record, table_file::TableFile};

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub unique: bool,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, nullable: bool, unique: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            nullable,
            unique,
        }
    }
}

impl PartialEq<DataType> for Column {
    fn eq(&self, other: &DataType) -> bool {
        let u8_data_type: u8 = min((&self.data_type).into(), 0x0C);
        let u8_other: u8 = min(other.into(), 0x0C);
        u8_data_type == u8_other || (self.nullable && other == &DataType::Null)
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub dir: String,
    table_file: TableFile,
}

impl Table {
    /// Creates a new [`Table`].
    pub fn new(name: &str, columns: Vec<Column>, dir: &str) -> Table {
        let table_file = TableFile::new(name, dir);
        Table {
            name: name.to_string(),
            columns,
            dir: dir.to_string(),
            table_file,
        }
    }

    pub fn search(
        &mut self,
        column_name: Option<&str>,
        value: DataType,
        operator: &str,
    ) -> Result<Vec<Record>, String> {
        if let Some(column_name) = column_name {
            if let Some(mut index_file) = self.get_index_file(column_name) {
                let record_ids = index_file.search(&value, operator);
                Ok(record_ids
                    .iter()
                    .filter_map(|id| self.table_file.get_record(*id))
                    .collect())
            } else {
                let column_index = self
                    .columns
                    .iter()
                    .position(|c| c.name == column_name)
                    .map(|i| i as u32);
                if column_index.is_none() {
                    return Err(format!("Column {} not found", column_name));
                }
                Ok(self.table_file.search(column_index, value, operator))
            }
        } else {
            Ok(self.table_file.search(None, value, operator))
        }
    }

    pub fn get_index_file(&self, column_name: &str) -> Option<IndexFile> {
        self.columns.iter().position(|c| c.name == column_name)?;
        let index_file_path = format!("{}/{}.{}.ndx", self.dir, self.name, column_name);
        let path = std::path::Path::new(&index_file_path);
        if path.exists() {
            Some(IndexFile::new(&self.name, column_name, &self.dir))
        } else {
            None
        }
    }

    pub fn insert(&mut self, values: Vec<DataType>) -> Result<(), String> {
        let next_row_id = self.table_file.get_last_row_id() + 1;
        for (i, column) in self.columns.clone().iter().enumerate() {
            if *column != values[i] {
                return Err(format!(
                    "Invalid data type for column {}. Expected {:?}, got {:?}",
                    column.name, column.data_type, values[i]
                ));
            }
            if column.unique || self.get_index_file(&column.name).is_some() {
                let records = self.search(Some(&column.name), values[i].clone(), "=")?;
                if !records.is_empty() {
                    return Err(format!(
                        "Value {} already exists in column {}",
                        values[i], column.name
                    ));
                }
            }
        }
        let record = Record::new(values, next_row_id);
        self.table_file.append_record(record);
        Ok(())
    }

    /// Deletes records from the table.
    ///
    /// Args:
    ///     * `column_name` - The name of the column to search.
    ///     * `value` - The value to search for.
    ///     * `operator` - The operator to use for the search.
    /// Returns:
    ///     * [`Result<usize, String>`] - The number of records deleted. Err if the column is not found.
    pub fn delete(
        &mut self,
        column_name: Option<&str>,
        value: &DataType,
        operator: &str,
    ) -> Result<usize, String> {
        let records = self.search(column_name, value.clone(), operator)?;
        for record in &records {
            self.table_file.delete_record(record.row_id);
            for (i, column) in self.columns.iter().enumerate() {
                if let Some(mut index_file) = self.get_index_file(&column.name) {
                    index_file.remove_item_from_cell(record.row_id, &record.values[i]);
                }
            }
        }
        Ok(records.len())
    }

    /// Updates records in the table.
    ///
    /// Args:
    ///    * `search_column` - The name of the column to search.
    ///    * `search_value` - The value to search for.
    ///    * `search_operator` - The operator to use for the search.
    ///    * `update_column` - The name of the column to update.
    ///    * `update_value` - The value to update.
    ///
    /// Returns:
    ///   * [`Result<usize, String>`] - The number of records updated. Err if the column is not found.
    pub fn update(
        &mut self,
        search_column: Option<&str>,
        search_value: DataType,
        search_operator: &str,
        update_column: &str,
        update_value: DataType,
    ) -> Result<usize, String> {
        let records = self.search(search_column, search_value, search_operator)?;
        let len = records.len();
        for record in records {
            let column_index = self.column_name_to_index(update_column)? as u32;
            self.table_file
                .update_record(record.row_id, column_index, update_value.clone());
            if let Some(mut index_file) = self.get_index_file(update_column) {
                index_file.update_record(
                    record.row_id,
                    &record.values[column_index as usize],
                    &update_value,
                );
            }
        }
        Ok(len)
    }

    pub fn column_name_to_index(&self, column_name: &str) -> Result<usize, String> {
        self.columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| format!("Column {} not found", column_name))
    }

    pub fn create_index(&mut self, column_name: &str) -> Result<IndexFile, String> {
        if self.get_index_file(column_name).is_some() {
            return Err(format!("Index for column {} already exists", column_name));
        }
        let column = self
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or(format!("Column {} not found", column_name))?;
        let mut index_file = IndexFile::new(&self.name, column_name, &self.dir);
        let values = self.search(None, DataType::Null, "=")?;
        index_file.initialize_index(values, column);
        Ok(index_file)
    }

    pub fn drop_index(&self, column_name: &str) -> Result<(), String> {
        let index_file_path = format!("{}/{}.{}.ndx", self.dir, self.name, column_name);
        if self.get_index_file(column_name).is_some() {
            std::fs::remove_file(index_file_path).expect("Failed removing index file");
            Ok(())
        } else {
            Err(format!("Index for column {} does not exist", column_name))
        }
    }

    pub fn has_index(&self) -> Option<&str> {
        for column in &self.columns {
            if self.get_index_file(&column.name).is_some() {
                return Some(&column.name);
            }
        }
        None
    }

    pub fn len(&mut self) -> usize {
        self.search(None, DataType::Null, "=").unwrap().len()
    }

    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{setup_records, setup_table, setup_table_no_records, teardown};

    #[test]
    fn test_new_table() {
        let mut table = setup_table("test_new_table", "data/testdata.txt");
        assert_eq!(table.len(), 10);
        teardown("test_new_table");
    }

    #[test]
    fn test_long_table() {
        let mut table = setup_table("test_long_table", "data/longdata.txt");
        assert_eq!(table.len(), 1000);
        teardown("test_long_table");
    }

    #[test]
    fn test_empty_table() {
        let mut table = setup_table_no_records("test_empty_table");
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        teardown("test_empty_table");
    }

    #[test]
    fn test_insert() {
        let mut table = setup_table_no_records("test_insert");
        let records = setup_records("data/testdata.txt");
        table.insert(records[0].values.clone()).expect("Error inserting record");
        assert_eq!(table.len(), 1);
        teardown("test_insert");
    }

    #[test]
    fn test_search() {
        let mut table = setup_table("test_search", "data/testdata.txt");
        let real_records = setup_records("data/testdata.txt");
        let records = table.search(Some("name"), real_records[0].values[0].clone(), "=").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].values, real_records[0].values);
        teardown("test_search");
    }
}
