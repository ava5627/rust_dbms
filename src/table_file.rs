use std::cmp::max;
use std::fmt::{Debug, Write as _};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use owo_colors::OwoColorize;

use crate::constants::{DataType, PageType, PAGE_SIZE};
use crate::database_file::DatabaseFile;
use crate::read_write_types::ReadWriteTypes;
use crate::record::Record;
use crate::utils::rainbow;

pub struct TableFile {
    file: File,
}

impl DatabaseFile for TableFile {
    fn len(&self) -> u64 {
        self.file
            .metadata()
            .expect("error getting file metadata")
            .len()
    }
    fn set_len(&self, length: u64) {
        self.file
            .set_len(length)
            .expect("Error setting file length");
    }
}

impl ReadWriteTypes for TableFile {}

impl Read for TableFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Write for TableFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Seek for TableFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl TableFile {
    pub fn new(table_name: &str, dir: &str) -> Self {
        let path = format!("{}/{}.tbl", dir, table_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .expect("Error opening table file");
        let mut table_file = TableFile { file };
        if table_file.len() == 0 {
            table_file.create_page(0xFFFFFFFF, PageType::TableLeaf);
        }
        table_file
    }

    fn get_min_row_id(&mut self, page: u32) -> u32 {
        if self.get_num_cells(page) == 0 {
            panic!("Page {} has no cells", page);
        }
        let page_type = self.get_page_type(page);
        let offset = self.get_cell_offset(page, 0);
        self.seek_to_page_offset(page, offset);
        match page_type {
            PageType::TableLeaf => self.skip_bytes(2),
            PageType::TableInterior => self.skip_bytes(4),
            _ => unreachable!("Page {} is not a table page", page),
        };
        self.read_u32()
    }

    pub fn get_last_row_id(&mut self) -> u32 {
        let page = self.get_last_leaf_page();
        if self.get_num_cells(page) == 0 {
            return 0;
        }
        let page_type = self.get_page_type(page);
        let offset = self.get_content_start(page);
        self.seek_to_page_offset(page, offset);
        match page_type {
            PageType::TableLeaf => self.skip_bytes(2),
            PageType::TableInterior => self.skip_bytes(4),
            _ => unreachable!("Page {} is not a table page", page),
        };
        self.read_u32()
    }

    fn split_page(&mut self, page: u32, split_row_id: u32) -> u32 {
        let page_type = self.get_page_type(page);
        let mut parent_page = self.get_parent_page(page);
        if parent_page == 0xFFFFFFFF {
            parent_page = self.create_page(0xFFFFFFFF, PageType::TableInterior);
            // 0x0A is the offset of the Parent Page pointer
            self.seek_to_page_offset(page, 0x0A);
            self.write_u32(parent_page);
            let min_row_id = self.get_min_row_id(page);
            self.write_page_pointer(parent_page, page, min_row_id);
        }
        let new_page = self.create_page(parent_page, page_type);
        self.write_page_pointer(parent_page, new_page, split_row_id);
        if page_type == PageType::TableLeaf {
            // 0x06 is the offset of the right most child pointer
            self.seek_to_page_offset(page, 0x06);
            self.write_u32(new_page);
        }
        new_page
    }

    fn write_page_pointer(&mut self, page: u32, pointer: u32, row_id: u32) {
        let cell_size = 8;
        let mut page = page;
        if self.should_split(page, cell_size) {
            page = self.split_page(page, row_id);
            self.seek_to_page_offset(pointer, 0x0A);
            self.write_u32(page); // Update parent page pointer
        }
        let content_start = self.set_content_start(page, cell_size);
        let num_cells = self.increment_num_cells(page);
        self.seek_to_page_offset(page, 0x06);
        self.write_u32(pointer);
        self.seek_to_page_offset(page, 0x0E + num_cells * 2);
        self.write_u32(content_start as u32);
        self.seek_to_page_offset(page, content_start);
        self.write_u32(pointer);
        self.write_u32(row_id);
    }

    fn write_record(&mut self, record: Record, page: u32) {
        let mut page = page;
        let cell_size = record.record_size + 6;
        if self.should_split(page, cell_size as i32) {
            page = self.split_page(page, record.row_id);
        }
        let content_start = self.set_content_start(page, cell_size as i32);
        let num_cells = self.increment_num_cells(page);
        // 0x0E is 2 bytes before the first cell pointer since num_cells is at least 1
        self.seek_to_page_offset(page, 0x0E + num_cells * 2);
        self.write_u32(content_start as u32);
        self.seek_to_page_offset(page, content_start);
        self.write_u16(record.record_size);
        self.write_u32(record.row_id);
        self.write_all(&record.header)
            .expect("Error writing record header");
        for value in record.values {
            self.write_value(value);
        }
    }

    pub fn append_record(&mut self, record: Record) {
        let page = self.get_last_leaf_page();
        self.write_record(record, page);
    }

    pub fn update_record(&mut self, row_id: u32, column_index: u32, value: DataType) {
        let (mut page, mut index) = self.find_record(row_id).expect("Record not found");
        let mut offset = self.get_cell_offset(page, index);
        let mut record = self.read_record(page, offset);
        if let DataType::Text(v) = &record.values[column_index as usize] {
            let old_size = v.len() as u16;
            let new_size = match value {
                DataType::Text(ref v_new) => v_new.len(),
                _ => unreachable!("Value is not text while column is text"),
            } as u16;
            if self.should_split(page, new_size as i32 - old_size as i32) {
                self.split_page(page, row_id);
                (page, index) = self.find_record(row_id).expect("Record not found");
            }
            self.shift_cells(page, index as i32 - 1, new_size as i32 - old_size as i32, 0);
            offset = self.get_cell_offset(page, index);
        }
        record.values[column_index as usize] = value;
        let new_record = Record::new(record.values, row_id);
        // 0x06 is the offset of the right most child pointer
        self.seek_to_page_offset(page, offset + 0x06);
        self.write_all(&new_record.header)
            .expect("Error writing record header");
        for value in new_record.values {
            self.write_value(value);
        }
    }

    pub fn delete_record(&mut self, row_id: u32) {
        let (page, index) = self.find_record(row_id).expect("Record not found");
        let offset = self.get_cell_offset(page, index);
        self.seek_to_page_offset(page, offset);
        let payload_size = self.read_u16();
        self.shift_cells(page, index as i32 - 1, -(payload_size as i32 + 6), -1);
        let parent_page = self.get_parent_page(page);
        if index == 0 && parent_page != 0xFFFFFFFF {
            let min_row_id = self.get_min_row_id(page);
            self.update_page_pointer(parent_page, index, min_row_id);
        }
        let num_cells = self.get_num_cells(page);
        // 0x02 is the offset of the number of cells
        self.seek_to_page_offset(page, 0x02);
        self.write_u16(num_cells - 1);
        let content_start = self.get_content_start(page);
        // 0x10 is the offset of the first cell pointer
        let cell_pointer_list_end = 0x10 + num_cells * 2;
        self.seek_to_page_offset(page, cell_pointer_list_end);
        for _ in cell_pointer_list_end..content_start {
            self.write_u8(0);
        }
    }

    fn update_page_pointer(&mut self, page: u32, index: u16, new_row_id: u32) {
        let offset = self.get_cell_offset(page, index);
        self.seek_to_page_offset(page, offset + 0x04);
        let old_row_id = self.read_u32();
        self.seek_to_page_offset(page, offset + 0x04);
        self.write_u32(new_row_id);
        if index == 0 {
            let parent_page = self.get_parent_page(page);
            let parent_index = self.find_record_on_page(parent_page, old_row_id);
            self.update_page_pointer(parent_page, parent_index, new_row_id);
        }
    }

    fn get_last_leaf_page(&mut self) -> u32 {
        if self.len() < PAGE_SIZE {
            panic!("Table file is empty");
        }
        if self.len() == PAGE_SIZE {
            return 0;
        }
        let mut next_page = self.get_root_page();
        loop {
            let page_type = self.get_page_type(next_page);
            if page_type == PageType::TableLeaf {
                break;
            }
            // 0x06 is the offset of the right most child pointer
            self.seek_to_page_offset(next_page, 0x06);
            next_page = self.read_u32();
        }
        next_page
    }

    pub fn get_record(&mut self, row_id: u32) -> Option<Record> {
        let (page, index) = match self.find_record(row_id) {
            Some((page, index)) => (page, index),
            None => return None,
        };
        let offset = self.get_cell_offset(page, index);
        Some(self.read_record(page, offset))
    }

    fn find_record_on_page(&mut self, page: u32, row_id: u32) -> u16 {
        let num_cells = self.get_num_cells(page);
        let mut current_cell = num_cells / 2;
        let mut low = 0;
        let mut high = num_cells - 1;
        let mut current_row_id = self.get_row_id(page, current_cell);
        while low < high {
            match current_row_id.cmp(&row_id) {
                std::cmp::Ordering::Less => low = current_cell + 1,
                std::cmp::Ordering::Greater => high = current_cell - 1,
                std::cmp::Ordering::Equal => break,
            }
            current_cell = (low + high).div_ceil(2);
            current_row_id = self.get_row_id(page, current_cell);
        }
        current_cell
    }

    fn find_record(&mut self, row_id: u32) -> Option<(u32, u16)> {
        let mut current_page = self.get_root_page();
        loop {
            let page_type = self.get_page_type(current_page);
            let current_cell = self.find_record_on_page(current_page, row_id);
            let current_row_id = self.get_row_id(current_page, current_cell);
            if page_type == PageType::TableLeaf {
                if current_row_id == row_id {
                    return Some((current_page, current_cell));
                } else {
                    return None;
                }
            } else if page_type == PageType::TableInterior {
                let offset = self.get_cell_offset(current_page, current_cell);
                self.seek_to_page_offset(current_page, offset);
                current_page = self.read_u32();
            } else {
                panic!("Page {} is not a table page", current_page);
            }
        }
    }

    fn get_row_id(&mut self, page: u32, current_cell: u16) -> u32 {
        let page_type = self.get_page_type(page);
        let offset = self.get_cell_offset(page, current_cell);
        self.seek_to_page_offset(page, offset);
        if page_type == PageType::TableLeaf {
            self.skip_bytes(2);
        } else {
            self.skip_bytes(4);
        }
        self.read_u32()
    }

    fn read_record(&mut self, page: u32, offset: u16) -> Record {
        self.seek_to_page_offset(page, offset + 2);
        let row_id = self.read_u32();
        let num_columns = self.read_u8();
        let mut columns: Vec<u8> = vec![0; num_columns as usize];
        self.read_exact(&mut columns)
            .expect("Error reading columns");
        let mut values: Vec<DataType> = vec![];
        for column in columns {
            values.push(self.read_value(column));
        }
        Record::new(values, row_id)
    }

    pub fn search(
        &mut self,
        column_id: Option<u32>,
        value: DataType,
        operator: &str,
    ) -> Vec<Record> {
        let mut records = vec![];
        let mut current_page = self.get_root_page();
        let mut page_type = self.get_page_type(current_page);
        while page_type != PageType::TableLeaf {
            let offset = self.get_cell_offset(current_page, 0);
            self.seek_to_page_offset(current_page, offset);
            current_page = self.read_u32();
            page_type = self.get_page_type(current_page);
        }

        while current_page != 0xFFFFFFFF {
            self.seek_to_page(current_page);
            let num_cells = self.get_num_cells(current_page);
            for i in 0..num_cells {
                let offset = self.get_cell_offset(current_page, i);
                let record = self.read_record(current_page, offset);
                if let Some(column_index) = column_id {
                    if record.compare_column(column_index as usize, &value, operator) {
                        records.push(record);
                    }
                } else {
                    records.push(record);
                }
            }
            self.seek_to_page_offset(current_page, 0x06);
            current_page = self.read_u32();
        }
        records
    }

    fn read_page_pointer(&mut self, page: u32, index: u16) -> (u32, u32) {
        let offset = self.get_cell_offset(page, index);
        self.seek_to_page_offset(page, offset);
        let pointer = self.read_u32();
        let row_id = self.read_u32();
        (pointer, row_id)
    }

    fn get_children(&mut self, page: u32) -> Vec<u32> {
        let mut children = vec![];
        let num_cells = self.get_num_cells(page);
        for i in 0..num_cells {
            let (pointer, _) = self.read_page_pointer(page, i);
            children.push(pointer);
        }
        children
    }

    pub fn graph(&mut self) {
        let root_page = self.get_root_page();
        println!("0x{:08X}", root_page.green());
        self.print_children(root_page, 1);
    }

    fn print_children(&mut self, page: u32, depth: u32) {
        let children = self.get_children(page);
        for child in children {
            if self.get_page_type(child) == PageType::TableLeaf {
                println!(
                    "{:|<indent$}0x{:08X}",
                    "",
                    child.blue(),
                    indent = depth as usize
                );
            } else {
                println!(
                    "{:|<indent$}0x{:08X}",
                    "",
                    child.green(),
                    indent = depth as usize
                );
                self.print_children(child, depth + 1);
            }
        }
    }

    pub fn print(&mut self) {
        let num_pages = self.len() / PAGE_SIZE;
        for i in 0..num_pages {
            self.print_page(i as u32);
        }
    }

    pub fn print_page(&mut self, page: u32) {
        self.seek_to_page(page);
        let page_type = self.read_u8();
        let unused_space = self.read_u8();
        let num_cells = self.read_u16();
        let content_start = self.read_u16();
        let next_page = self.read_u32();
        let parent_page = self.read_u32();
        let unused_2 = self.read_u16();

        let page_start = page as u64 * PAGE_SIZE;
        println!(
            "{:<4}{:04X} {}, XX, {}, {}, {}, {}",
            page,
            page,
            "Page Type".green(),
            "Num Cells".blue(),
            "Content Start".purple(),
            "Next Page".yellow(),
            "Parent Page".cyan()
        );
        print!(
            "{:08X}  {:02X} {:02X} {:04X} {:04X} {:08X} {:08X} {:04X}",
            page_start,
            page_type.green(),
            unused_space,
            num_cells.blue(),
            content_start.purple(),
            next_page.yellow(),
            parent_page.cyan(),
            unused_2
        );
        print!(" | ");
        let page_type: PageType = page_type.into();
        print!("{:?} ", page_type.green());
        print!("{} ", num_cells.blue());
        print!("{} ", content_start.purple());
        println!();
        let mut offsets: Vec<u16> = vec![0; num_cells as usize];
        if num_cells > 0 {
            print!("{:08X}  ", 0x10 + page_start);
            for i in 0..num_cells {
                let offset = self.read_u16();
                offsets[i as usize] = offset;
                print!("{} ", rainbow(&format!("{:04X}", offset), i as usize));
            }
            println!();
            if page_type == PageType::TableLeaf {
                self.print_leaf_cells(page, offsets);
            } else {
                self.print_inner_cells(page, offsets);
            }
        }
    }

    fn print_inner_cells(&mut self, page: u32, offsets: Vec<u16>) {
        let page_start = page * PAGE_SIZE as u32;
        println!("{:8}  {:8} {:8}", "", "Page".blue(), "Row ID".yellow());
        for (i, &o) in offsets.iter().enumerate() {
            self.seek_to_page_offset(page, o);
            let page = self.read_u32();
            let row_id = self.read_u32();
            print!(
                "{}  ",
                rainbow(&format!("{:08X}", o as u32 + page_start), i)
            );
            println!("{:08X} {:08X}", page.blue(), row_id.yellow());
        }
    }

    fn print_leaf_cells(&mut self, page: u32, offsets: Vec<u16>) {
        let page_start = page * PAGE_SIZE as u32;
        let mut records = vec![];
        for i in 0..offsets.len() {
            let o = offsets[i];
            self.seek_to_page_offset(page, o);
            let record_size = self.read_u16();
            if i > 0 && o + record_size + 6 > offsets[i - 1] {
                records.push(None);
            } else {
                let record = self.read_record(page, o);
                records.push(Some(record));
            }
        }
        let num_columns = records
            .iter()
            .find(|r| r.is_some())
            .unwrap()
            .as_ref()
            .unwrap()
            .values
            .len();
        let mut column_widths = vec![0; num_columns];
        for record in records.iter().flatten() {
            for (i, value) in record.values.iter().enumerate() {
                column_widths[i] = max(column_widths[i], value.to_string().len());
            }
        }
        println!(
            "          {:4} {:8} {:width$} Values",
            "Size".blue(),
            "Row ID".yellow(),
            "Columns",
            width = column_widths.len() * 3 - 1
        );
        for (j, (record, &o)) in records.iter().zip(&offsets).enumerate().rev() {
            print!(
                "{}  ",
                rainbow(&format!("{:08X}", o as u32 + page_start), j)
            );
            if record.is_none() {
                print!("{}", "MALFORMED RECORD:".on_red());
                let num_malfomed_bytes = offsets[j - 1] - o;
                let mut malformed_bytes = vec![0; num_malfomed_bytes as usize];
                self.seek_to_page_offset(page, o);
                self.read_exact(&mut malformed_bytes)
                    .expect("Error reading malformed bytes");
                let record_size = u16::from_le_bytes([malformed_bytes[0], malformed_bytes[1]]);
                println!(
                    " should be {} bytes but is {} {}",
                    record_size,
                    malformed_bytes.len(),
                    malformed_bytes.iter().fold("".to_string(), |mut acc, b| {
                        let _ = write!(acc, "{:02X} ", b.on_red());
                        acc
                    })
                );
                continue;
            }
            let record = record.as_ref().unwrap();
            print!("{:04X} ", record.record_size.blue());
            print!("{:08X} ", record.row_id.yellow());
            for (i, column) in record.values.iter().enumerate() {
                let col_u8: u8 = column.into();
                print!("{} ", rainbow(&format!("{:02X}", col_u8), i));
            }
            for (i, value) in record.values.iter().enumerate() {
                let value_str = rainbow(&format!("{:width$}", value, width = column_widths[i]), i);
                print!("{} ", value_str);
            }
            println!();
            if j > 0 && o + record.record_size + 6 < offsets[j - 1] {
                self.seek_to_page_offset(page, o + record.record_size + 6);
                let num_missing_bytes = offsets[j - 1] - o - record.record_size - 6;
                let mut missing_bytes = vec![0; num_missing_bytes as usize];
                self.read_exact(&mut missing_bytes)
                    .expect("Error reading missing bytes");
                let o2 = o as u32 + record.record_size as u32 + 6 + page_start;
                println!(
                    "{:08X}  {}",
                    o2,
                    missing_bytes.iter().fold("".to_string(), |mut acc, b| {
                        let _ = write!(acc, "{:02X} ", b.on_red());
                        acc
                    })
                );
            }
        }
    }
}

impl Debug for TableFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableFile")
    }
}

#[cfg(test)]
mod tests {
    use crate::constants::PageType;
    use crate::database_file::DatabaseFile;
    use std::fs::remove_file;
    use std::{fs::File, io::BufRead};

    use crate::{constants::DataType, record::Record};

    use super::TableFile;

    fn setup(test_name: &str) -> (TableFile, Vec<Record>) {
        setup_table(test_name, "data/testdata.txt")
    }

    fn setup_table(test_name: &str, test_file_path: &str) -> (TableFile, Vec<Record>) {
        let file_name = format!("data/{}.tbl", test_name);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test table file");
        }
        let table_file = TableFile::new(test_name, "data");
        let columns = [
            DataType::Text("".to_string()),
            DataType::Text("".to_string()),
            DataType::Int(0),
            DataType::Text("".to_string()),
        ];
        let mut records = vec![];
        let test_file = File::open(test_file_path).expect("Error opening test data file");
        let test_data = std::io::BufReader::new(test_file);
        for (i, line) in test_data.lines().enumerate() {
            let line = line.expect("Error reading line");
            let str_values: Vec<&str> = line.split(';').collect();
            let mut values = vec![];
            for (j, value) in str_values.iter().enumerate() {
                let v = match DataType::parse_str(columns[j].clone(), value) {
                    Ok(v) => v,
                    Err(e) => panic!("Error parsing value: {}", e),
                };
                values.push(v);
            }
            let record = Record::new(values, i as u32);
            records.push(record);
        }
        (table_file, records)
    }

    fn tear_down(test_name: &str) {
        let file_name = format!("data/{}.tbl", test_name);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test table file");
        }
    }

    #[test]
    fn test_append_record() {
        let (mut table_file, records) = setup("test_append_record");
        for record in records {
            table_file.append_record(record);
        }
        assert_eq!(2048, table_file.len());
        tear_down("test_append_record");
    }

    #[test]
    fn test_get_min_row_id() {
        let (mut table_file, records) = setup("test_get_min_row_id");
        for record in records {
            table_file.append_record(record);
        }
        assert_eq!(0, table_file.get_min_row_id(0));
        assert_eq!(0, table_file.get_min_row_id(1));
        assert_eq!(4, table_file.get_min_row_id(2));
        assert_eq!(8, table_file.get_min_row_id(3));
        tear_down("test_get_min_row_id");
    }

    #[test]
    fn test_write_page_pointer() {
        let (mut table_file, _) = setup("test_write_page_pointer");
        table_file.create_page(0, PageType::TableInterior);
        table_file.write_page_pointer(1, 0, 4);
        assert_eq!(4, table_file.get_min_row_id(1));
        tear_down("test_write_page_pointer");
    }

    #[test]
    fn test_write_record() {
        let (mut table_file, records) = setup("test_write_record");
        table_file.write_record(records[0].clone(), 0);
        let cell_size = records[0].record_size + 6;
        assert_eq!(512, table_file.len());
        let (page_type, num_cells, content_start, right_most_child, parent_page) =
            table_file.get_page_info(0);
        assert_eq!(PageType::TableLeaf, page_type);
        assert_eq!(1, num_cells);
        assert_eq!(512 - cell_size, content_start);
        assert_eq!(0xFFFFFFFF, right_most_child);
        assert_eq!(0xFFFFFFFF, parent_page);
        tear_down("test_write_record");
    }

    #[test]
    fn test_get_last_leaf_page() {
        let (mut table_file, records) = setup("test_get_last_leaf_page");
        for record in records {
            table_file.append_record(record);
        }
        assert_eq!(3, table_file.get_last_leaf_page());
        tear_down("test_get_last_leaf_page");
    }

    #[test]
    fn test_get_record() {
        let (mut table_file, records) = setup("test_get_record");
        for record in &records {
            table_file.append_record(record.clone());
        }
        let record = table_file.get_record(1).expect("Error getting record");
        assert_eq!(records[1], record);
        tear_down("test_get_record");
    }

    #[test]
    fn test_find_record_on_page() {
        let (mut table_file, records) = setup("test_find_record_on_page");
        for record in &records {
            table_file.append_record(record.clone());
        }
        assert_eq!(1, table_file.find_record_on_page(0, 1));
        tear_down("test_find_record_on_page");
    }

    #[test]
    fn test_shift_cells() {
        let (mut table_file, records) = setup("test_shift_cells");
        for record in &records {
            table_file.append_record(record.clone());
        }
        let original_offset = table_file.get_cell_offset(3, 1);
        table_file.shift_cells(3, 0, 50, 0);
        assert_eq!(original_offset - 50, table_file.get_cell_offset(3, 1));
        tear_down("test_shift_cells");
    }

    #[test]
    fn test_update_record() {
        let (mut table_file, records) = setup("test_update_record");
        for record in &records {
            table_file.append_record(record.clone());
        }
        let new_text =
            DataType::Text("TEST UPDATE RECORD ABC DEF GHI JKL MNO PQR STU VWX YZ".to_string());
        table_file.update_record(9, 1, new_text.clone());
        let updated_record = table_file.get_record(9).expect("Error getting record");
        assert_eq!(new_text, updated_record.values[1]);
        tear_down("test_update_record");
    }

    #[test]
    fn test_delete_record() {
        let (mut table_file, records) = setup("test_delete_record");
        for record in &records {
            table_file.append_record(record.clone());
        }
        table_file.delete_record(2);
        let should_not_exist = table_file.get_record(2);
        assert!(should_not_exist.is_none());
        let serch_result = table_file.search(Some(2), DataType::Int(9800), "=");
        assert_eq!(0, serch_result.len());
        tear_down("test_delete_record");
    }

    #[test]
    fn test_search() {
        let (mut table_file, records) = setup("test_search");
        let search_value = DataType::Int(8000);
        let real_results = records
            .iter()
            .filter(|r| r.values[2] >= search_value)
            .collect::<Vec<&Record>>();
        for record in &records {
            table_file.append_record(record.clone());
        }
        let search_results = table_file.search(Some(2), search_value, ">=");
        assert_eq!(real_results.len(), search_results.len());
        for (real_result, search_result) in real_results.iter().zip(search_results.iter()) {
            assert_eq!(*real_result, search_result);
        }
        tear_down("test_search");
    }

    #[test]
    fn test_split_page() {
        let (mut table_file, records) = setup("test_split_page");
        for record in &records {
            table_file.append_record(record.clone());
        }
        table_file.split_page(3, 9);
        let (page_type, num_cells, content_start, right_most_child, parent_page) =
            table_file.get_page_info(4);
        assert_eq!(PageType::TableLeaf, page_type);
        assert_eq!(0, num_cells);
        assert_eq!(0x200, content_start);
        assert_eq!(0xFFFFFFFF, right_most_child);
        assert_eq!(1, parent_page);
        tear_down("test_split_page");
    }

    #[test]
    fn test_long_file() {
        let (mut table_file, records) = setup_table("test_long_file", "data/longdata.txt");
        for record in &records {
            table_file.append_record(record.clone());
        }
        assert_eq!(table_file.get_last_leaf_page(), 0x9D);
        assert_eq!(table_file.get_root_page(), 0x33);
        let search_results = table_file.search(Some(2), DataType::Int(800), "<>");
        assert_eq!(1000, search_results.len());
        tear_down("test_long_file");
    }
}
