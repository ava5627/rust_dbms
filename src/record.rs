use std::fmt::Display;

use crate::constants::DataType;

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub record_size: u16,
    pub values: Vec<DataType>,
    pub row_id: u32,
    pub header: Vec<u8>,
}

impl Record {
    pub fn new(values: Vec<DataType>, row_id: u32) -> Record {
        let mut record_size = values.len() as u16 + 1;
        let mut header = vec![];
        header.push(values.len() as u8);
        for value in &values {
            record_size += value.size();
            header.push(value.into());
        }
        Record {
            record_size,
            values,
            row_id,
            header,
        }
    }

    pub fn compare_column(&self, column: usize, value: &DataType, operator: &str) -> bool {
        let column_value = &self.values[column];
        match operator {
            "=" => column_value == value,
            "<>" => column_value != value,
            "<" => column_value < value,
            "<=" => column_value <= value,
            ">" => column_value > value,
            ">=" => column_value >= value,
            _ => unreachable!("Invalid operator"),
        }
    }

    pub fn column_widths(&self) -> Vec<usize> {
        self.values.iter().map(|v| format!("{}", v).len()).collect()
    }

    pub fn print_columns(&self, columns: &[usize], widths: &[usize]) -> String {
        if columns.len() != widths.len() {
            panic!("Columns and widths must have the same length");
        }
        let mut result = String::new();
        for (column, w) in columns.iter().zip(widths.iter()) {
            // print!("{:width$} ", self.values[*column], width = *w);
            result.push_str(&format!("{:width$} ", self.values[*column], width = *w));
        }
        result
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record(row_id: {}, values: [", self.row_id)?;
        for value in &self.values {
            write!(f, "\n\t{}, ", value)?;
        }
        write!(f, "])")?;
        Ok(())
    }
}
