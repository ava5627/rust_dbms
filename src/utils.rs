use std::{fs::File, io::BufRead};

use owo_colors::{AnsiColors, OwoColorize};

use crate::{
    constants::DataType,
    record::Record,
    table::{Column, Table},
};

pub fn rainbow(content: &str, num: usize) -> String {
    let colors = ["magenta", "cyan", "green", "blue", "white", "yellow", "red"];
    let color: AnsiColors = colors[num % colors.len()].into();
    return content.color(color).to_string();
}

pub fn setup_columns() -> Vec<Column> {
    vec![
        Column::new("name", DataType::Text(Default::default()), false, false),
        Column::new("address", DataType::Text(Default::default()), false, false),
        Column::new("age", DataType::Int(Default::default()), false, false),
        Column::new("email", DataType::Text(Default::default()), false, false),
    ]
}

pub fn setup_records(test_file_path: &str) -> Vec<Record> {
    let columns = setup_columns();
    let mut records = vec![];
    let test_file = File::open(test_file_path).expect("Error opening test data file");
    let test_data = std::io::BufReader::new(test_file);
    for (i, line) in test_data.lines().enumerate() {
        let line =
            line.unwrap_or_else(|_| panic!("Error reading line in file: {}", test_file_path));
        let str_values: Vec<&str> = line.split(';').collect();
        let mut values = vec![];
        for (j, value) in str_values.iter().enumerate() {
            let v = match DataType::parse_str(columns[j].data_type.clone(), value) {
                Ok(v) => v,
                Err(e) => panic!(
                    "{}: Error parsing {} into {:?}",
                    e, value, columns[j].data_type
                ),
            };
            values.push(v);
        }
        let record = Record::new(values, i as u32);
        records.push(record);
    }
    records
}

pub fn setup_table(test_name: &str, test_file_path: &str) -> crate::table::Table {
    let test_dir = "data/test";
    let test_dir_path = std::path::Path::new(test_dir);
    if !test_dir_path.exists() {
        std::fs::create_dir_all(test_dir).expect("Error creating test directory");
    }
    let columns = setup_columns();
    let table_file_path = format!("{}/{}.tbl", test_dir, test_name);
    if std::path::Path::new(&table_file_path).exists() {
        teardown(test_name);
    }
    let mut table = Table::new(test_name, columns, test_dir);
    let records = setup_records(test_file_path);
    for record in records {
        table.insert(record.values).expect("Error inserting record");
    }
    table
}

pub fn setup_table_no_records(test_name: &str) -> crate::table::Table {
    let test_dir = "data/test";
    let test_dir_path = std::path::Path::new(test_dir);
    if !test_dir_path.exists() {
        std::fs::create_dir_all(test_dir).expect("Error creating test directory");
    }
    let columns = setup_columns();
    Table::new(test_name, columns, test_dir)
}

pub fn teardown(test_name: &str) {
    let test_dir = "data/test";
    let test_dir_path = std::path::Path::new(test_dir);
    if !test_dir_path.exists() {
        panic!("Test directory does not exist");
    }
    let table_file_path = format!("data/test/{}.tbl", test_name);
    if std::path::Path::new(&table_file_path).exists() {
        std::fs::remove_file(table_file_path).expect("Failed removing table file");
    }
    // Remove index files with pattern data/test/test_name.*.ndx
    if let Ok(entries) = std::fs::read_dir(test_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name().into_string().unwrap();
            let tn = format!("{}.", test_name);
            if file_name.starts_with(&tn) && file_name.ends_with("ndx") {
                std::fs::remove_file(entry.path()).expect("Failed removing index file");
            }
        }
    }
}
