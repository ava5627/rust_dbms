#![allow(dead_code)]
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

use owo_colors::OwoColorize;

use crate::constants::PAGE_SIZE;
use crate::record::Record;
use crate::utils::rainbow;
use crate::{
    constants::{DataType, PageType},
    database_file::DatabaseFile,
    read_write_types::ReadWriteTypes,
    table::Table,
};

pub struct IndexFile {
    data_type: DataType,
    file: File,
    column_index: usize,
}

impl DatabaseFile for IndexFile {
    fn set_len(&self, length: u64) {
        self.file
            .set_len(length)
            .expect("Error setting file length");
    }

    fn len(&self) -> u64 {
        self.file.metadata().expect("Error getting metadata").len()
    }
}

impl ReadWriteTypes for IndexFile {}

impl Read for IndexFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.file.read(buf)
    }
}

impl Write for IndexFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()
    }
}

impl Seek for IndexFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64, std::io::Error> {
        self.file.seek(pos)
    }
}

impl IndexFile {
    pub fn new(Table { name, columns, .. }: &Table, column_index: usize, dir: &str) -> Self {
        let path = format!("{}/{}.{}.ndx", dir, name, columns[column_index].name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Error opening index file");
        let data_type = columns[column_index].data_type.clone();
        let mut idx = Self {
            data_type,
            file,
            column_index,
        };
        if idx.len() == 0 {
            idx.create_page(0xFFFFFFFF, PageType::IndexLeaf);
        }
        idx
    }

    /// Reads a value from the index file at `page` and `offset`.
    ///
    /// Args:
    ///     * `page` - The page number to read from.
    ///     * `offset` - The offset within the page to read from.
    /// Returns:
    ///     * [`Option<DataType>`]: The value read from the index file. Returns [`None`] if the payload size is 0.
    pub fn read_index_value(&mut self, page: u32, offset: u16) -> Option<DataType> {
        let page_type = self.get_page_type(page);
        self.seek_to_page_offset(page, offset);
        if page_type == PageType::IndexInterior {
            self.skip_bytes(4);
        }
        let payload_size = self.read_u16();
        if payload_size == 0 {
            return None;
        }
        self.skip_bytes(1);
        let payload_type = self.read_u8();
        let value = self.read_value(payload_type);
        Some(value)
    }

    /// Reads a full index value from the index file at `page` and `offset`.
    ///
    /// Args:
    ///    * `page` - The page number to read from.
    ///    * `offset` - The offset within the page to read from.
    ///
    /// Returns: a tuple containing
    ///   * [`Option<DataType>`]: The Value, None if leftmost cell of an interior index page
    ///   * [`Option<u32>`]: The child page if the value is an interior index value.
    ///   * [`Vec<u32>`]: Row IDs contained in the child page
    pub fn read_full_index_value(
        &mut self,
        page: u32,
        offset: u16,
    ) -> (Option<DataType>, Option<u32>, Vec<u32>) {
        let page_type = self.get_page_type(page);
        self.seek_to_page_offset(page, offset);
        let child_page = match page_type {
            PageType::IndexInterior => Some(self.read_u32()),
            _ => None,
        };
        let payload_size = self.read_u16();
        if payload_size == 0 {
            return (None, child_page, vec![]);
        }
        let num_row_ids = self.read_u8();
        let payload_type = self.read_u8();
        let value = self.read_value(payload_type);
        if num_row_ids == 0 {
            return (Some(value), child_page, vec![]);
        }
        let row_ids = (0..num_row_ids)
            .map(|_| self.read_u32())
            .collect::<Vec<u32>>();
        (Some(value), child_page, row_ids)
    }

    pub fn create_interior_page(&mut self, parent: u32, child: u32) -> u32 {
        let page = self.create_page(parent, PageType::IndexInterior);

        self.seek_to_page_offset(page, 0x02);
        self.write_u16(1); // Set the number of cells to 1
        self.write_u16(PAGE_SIZE as u16 - 6); // Set the start of the content area
        self.write_u32(child); // Set pointer to the rightmost child page

        self.seek_to_page_offset(page, 0x10);
        self.write_u16(PAGE_SIZE as u16 - 6); // Set the offset of the first cell

        // Pointer to the leftmost page has no corresponding cell payload
        self.seek_to_page_offset(page, PAGE_SIZE as u16 - 6);
        self.write_u32(child); // Set the leftmost child page
        self.write_u8(0x00); // Set the payload size to 0
        page
    }

    pub fn split_page(&mut self, page: u32, split_value: &DataType) -> u32 {
        let page_type = self.get_page_type(page);
        let mut parent_page = self.get_parent_page(page);
        if parent_page == 0xFFFFFFFF {
            parent_page = self.create_interior_page(parent_page, page);
            // Set the parent page
            self.seek_to_page_offset(page, 0x0A);
            self.write_u32(parent_page);
        }

        let middle = self.get_num_cells(page) / 2;
        let middle_offset = self.get_cell_offset(page, middle);
        let (value, child_pointer, row_ids) = self.read_full_index_value(page, middle_offset);

        let new_page = match child_pointer {
            Some(p) => self.create_interior_page(parent_page, p),
            None => self.create_page(parent_page, PageType::IndexLeaf),
        };

        let value = match value {
            Some(v) => v,
            None => panic!("Value must be present"),
        };
        let num_row_ids = row_ids.len();
        self.write_cell(parent_page, &value, row_ids, new_page);

        self.seek_to_page_offset(page, middle_offset);
        let cell_size = self.payload_size(&value, num_row_ids)
            + 2
            + match page_type {
                PageType::IndexInterior => 4,
                _ => 0,
            };
        let zero_bytes = vec![0; cell_size as usize];
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");

        self.move_cells(page, new_page, middle);

        self.seek_to_page_offset(page, 0x10 + middle * 2);
        self.write_u16(0);

        let remaining_cells_offset = self.get_cell_offset(page, middle - 1);
        self.seek_to_page_offset(page, 0x02);

        self.write_u16(middle);
        self.write_u16(remaining_cells_offset);

        if split_value > &value {
            new_page
        } else {
            page
        }
    }

    fn find_value_position(&mut self, page: u32, value: &DataType) -> Option<u16> {
        let num_cells = self.get_num_cells(page);
        if num_cells == 0 {
            return None;
        }
        let mut mid = num_cells / 2;
        let mut low = 0;
        let mut high = num_cells - 1;
        while low < high {
            let current_offset = self.get_cell_offset(page, mid);
            let currtent_value = self.read_index_value(page, current_offset);
            if let Some(currtent_value) = currtent_value {
                if currtent_value == *value {
                    return Some(mid);
                } else if currtent_value < *value {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            } else {
                high = mid - 1;
            }
            mid = (low + high + 1) / 2;
        }
        Some(mid)
    }

    pub fn move_cells(&mut self, source_page: u32, destination_page: u32, preceding_cell: u16) {
        let cell_offset = self.get_cell_offset(source_page, preceding_cell);
        let num_cells = self.get_num_cells(source_page);
        let num_cells_to_move = num_cells - preceding_cell - 1;
        let content_start = self.get_content_start(source_page);

        // Read the bytes to be moved
        let mut cell_bytes = vec![0; (cell_offset - content_start) as usize];
        self.seek_to_page_offset(source_page, content_start);
        self.read_exact(&mut cell_bytes)
            .expect("Error reading cell bytes");

        // Read the offsets of the cells to be moved
        let mut cell_offsets = vec![0; num_cells_to_move as usize * 2];
        let offset_location = 0x10 + (preceding_cell + 1) * 2;
        self.seek_to_page_offset(source_page, offset_location);
        self.read_exact(&mut cell_offsets)
            .expect("Error reading cell offsets");

        // Overwrite the cells to be moved with zero bytes
        let zero_bytes = vec![0; cell_bytes.len()];
        self.seek_to_page_offset(destination_page, offset_location);
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");

        // Write the cells to the destination page
        let new_content_start = self.get_content_start(destination_page) - cell_bytes.len() as u16;
        self.seek_to_page_offset(destination_page, new_content_start);
        self.write_all(&cell_bytes)
            .expect("Error writing cell bytes");

        // Set the new content start on the destination page
        self.seek_to_page_offset(destination_page, 0x04);
        self.write_u16(new_content_start);

        // Write the modified offsets to the destination page
        let offset_diff = content_start as i32 - new_content_start as i32;
        self.seek_to_page_offset(destination_page, 0x10);
        let modified_offsets = cell_offsets
            .chunks(2)
            .map(|offset| (u16::from_le_bytes([offset[0], offset[1]]) as i32 - offset_diff as i32))
            .flat_map(|offset| (offset as u16).to_le_bytes().to_vec())
            .collect::<Vec<u8>>();
        self.write_all(&modified_offsets)
            .expect("Error writing modified offsets");

        // Update the number of cells on the destination page
        self.seek_to_page_offset(destination_page, 0x02);
        self.write_u16(num_cells_to_move);
        // Update the number of cells on the source page
        self.seek_to_page_offset(source_page, 0x02);
        self.write_u16(preceding_cell);

        // Fill the moved offsets with zero bytes
        let zero_bytes = vec![0; (cell_offset - content_start) as usize];
        self.seek_to_page_offset(source_page, content_start);
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");
    }

    fn initialize_index(&mut self, records: Vec<Record>) {
        let mut values_and_ids: Vec<(DataType, Vec<u32>)> = records
            .iter()
            .map(|record| {
                let value = record.values[self.column_index].clone();
                let id = record.row_id;
                (value, id)
            })
            .fold(
                Vec::new(),
                |mut acc: Vec<(DataType, Vec<u32>)>, (value, id)| {
                    let idx = acc.iter().position(|(v, _)| v == &value);
                    if let Some(idx) = idx {
                        acc[idx].1.push(id);
                    } else {
                        acc.push((value, vec![id]));
                    }
                    acc
                },
            );
        values_and_ids.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        for (value, ids) in values_and_ids {
            let page = match self.find_value(&value) {
                Ok((page, _)) => page,
                Err((page, _)) => page,
            };
            self.write_cell(page, &value, ids.to_vec(), 0xFFFFFFFF);
        }
    }

    fn write_cell(&mut self, page: u32, value: &DataType, row_ids: Vec<u32>, child_page: u32) {
        let page_type = self.get_page_type(page);
        let payload_size = self.payload_size(value, row_ids.len());
        let cell_size = payload_size
            + 2
            + if page_type == PageType::IndexInterior {
                4
            } else {
                0
            };

        let page = if self.should_split(page, cell_size as i32) {
            self.split_page(page, value)
        } else {
            page
        };

        let insert_point = match self.find_value_position(page, value) {
            Some(offset) => offset as i32,
            None => -1,
        };
        let offset = if insert_point == self.get_num_cells(page) as i32 {
            self.set_content_start(page, cell_size as i32)
        } else {
            self.shift_cells(page, insert_point, cell_size as i32, 1) as u16
        };
        self.increment_num_cells(page);

        self.seek_to_page_offset(page, 0x10 + 2 * (insert_point + 1) as u16);
        self.write_u16(offset);

        self.seek_to_page_offset(page, offset);
        if page_type == PageType::IndexInterior {
            if child_page == 0xFFFFFFFF {
                panic!("Child page must be set for interior index pages");
            }
            self.write_u32(child_page);
        }
        self.write_u16(payload_size);
        self.write_u8(row_ids.len() as u8);

        let value_type = value.into();
        self.write_u8(value_type);

        self.write_value(value.clone());
        let row_ids_bytes = row_ids
            .iter()
            .flat_map(|id| id.to_le_bytes().to_vec())
            .collect::<Vec<u8>>();
        self.write_all(&row_ids_bytes)
            .expect("Error writing row IDs");
    }

    fn update_record(&mut self, row_id: u32, old_value: &DataType, new_value: &DataType) {
        self.remove_item_from_cell(row_id, old_value);
        self.insert_item_into_cell(row_id, new_value);
    }

    fn remove_item_from_cell(&mut self, row_id: u32, value: &DataType) {
        let (page, index) = self
            .find_value(value)
            .expect("Cannot remove item that does not exist");
        let offset = self.get_cell_offset(page, index);
        let (_, _, mut row_ids) = self.read_full_index_value(page, offset);
        let id_index = match row_ids.iter().position(|id| id == &row_id) {
            Some(idx) => idx,
            None => panic!("Row ID not found in cell"),
        };
        row_ids.remove(id_index);
        let page_type = self.get_page_type(page);
        self.seek_to_page_offset(page, offset);
        if page_type == PageType::IndexInterior {
            self.skip_bytes(4);
        }
        let payload_size = self.read_u16() - 4;
        self.skip_bytes(-2);
        self.write_u16(payload_size);
        self.write_u8(row_ids.len() as u8);
        let value_size = value.size();
        self.skip_bytes(value_size as i64 + 1);
        let row_ids_bytes = row_ids
            .iter()
            .flat_map(|id| id.to_le_bytes().to_vec())
            .collect::<Vec<u8>>();
        self.write_all(&row_ids_bytes)
            .expect("Error writing row IDs");
        self.shift_cells(page, index as i32, -4, 0);

        if row_ids.is_empty() {
            self.remove_cell(page, index);
        }
    }

    fn remove_cell(&mut self, page: u32, index: u16) {
        let offset = self.get_cell_offset(page, index);
        let (_, _, row_ids) = self.read_full_index_value(page, offset);
        if !row_ids.is_empty() {
            panic!("Cannot remove cell with row IDs");
        }
        // TODO: Remove cell from page
        let page_type = self.get_page_type(page);
        match page_type {
            PageType::IndexInterior => self.remove_cell_from_interior(page, index),
            PageType::IndexLeaf => self.remove_cell_from_leaf(page, index),
            _ => unreachable!("Index pages must be either IndexInterior or IndexLeaf"),
        }
    }

    fn remove_cell_from_interior(&mut self, page: u32, index: u16) {
        let offset = self.get_cell_offset(page, index);
        self.seek_to_page_offset(page, offset);
    }

    fn remove_cell_from_leaf(&mut self, page: u32, index: u16) {
        let offset = self.get_cell_offset(page, index);
        self.seek_to_page_offset(page, offset);
    }

    fn insert_item_into_cell(&mut self, row_id: u32, new_value: &DataType) {
        let (mut page, mut index) = match self.find_value(new_value) {
            Ok((p, i)) => (p, i as i32),
            Err((p, _)) => {
                return self.write_cell(p, new_value, vec![row_id], 0xFFFFFFFF);
            }
        };
        if self.should_split(page, 4) {
            page = self.split_page(page, new_value);
            index = match self.find_value_position(page, new_value) {
                Some(offset) => offset as i32,
                None => unreachable!("Value has already been found in the page"),
            };
        }
        let page_type = self.get_page_type(page);
        self.shift_cells(page, index - 1, 4, 0);
        let new_offset = self.get_cell_offset(page, index as u16);
        let (_, _, mut row_ids) = self.read_full_index_value(page, new_offset);
        row_ids.push(row_id);
        row_ids.sort();
        self.seek_to_page_offset(page, new_offset);
        if page_type == PageType::IndexInterior {
            self.skip_bytes(4);
        }
        let payload_size = self.read_u16() + 4;
        self.skip_bytes(-2);
        self.write_u16(payload_size);
        self.write_u8(row_ids.len() as u8);
        self.skip_bytes(1);
        let value_size = new_value.size();
        self.skip_bytes(value_size as i64);
        let row_ids_bytes = row_ids
            .iter()
            .flat_map(|id| id.to_le_bytes().to_vec())
            .collect::<Vec<u8>>();
        self.write_all(&row_ids_bytes)
            .expect("Error writing row IDs");
    }

    fn find_value(&mut self, value: &DataType) -> Result<(u32, u16), (u32, u16)> {
        let mut current_page = self.get_root_page();
        loop {
            let index = match self.find_value_position(current_page, value) {
                Some(i) => i,
                None => {
                    return Err((current_page, 0));
                }
            };
            let offset = self.get_cell_offset(current_page, index);
            let (cell_value, child_page, _) = self.read_full_index_value(current_page, offset);
            if cell_value.as_ref() == Some(value) {
                return Ok((current_page, index));
            } else if let Some(child_page) = child_page {
                current_page = child_page;
            } else {
                return Err((current_page, index));
            }
        }
    }

    fn traverse(&mut self, page: u32, start: i32, end: i32, direction: i32) -> Vec<u32> {
        if start > end {
            return vec![];
        }
        let start = start as u16;
        let mut row_ids = vec![];
        let mut current_cell = start;
        let cur_offset = self.get_cell_offset(page, current_cell);
        while current_cell as i32 <= end {
            let offset = self.get_cell_offset(page, current_cell);
            let (_, child_page, cell_row_ids) = self.read_full_index_value(page, offset);
            row_ids.extend(cell_row_ids);
            if let Some(child_page) = child_page {
                let num_cells = self.get_num_cells(child_page);
                let new_row_ids = self.traverse(child_page, 0, num_cells as i32 - 1, direction);
                row_ids.extend(new_row_ids);
            }
            current_cell += 1;
        }

        let parent_page = self.get_parent_page(page);
        if parent_page == 0xFFFFFFFF {
            return row_ids;
        }

        let v = self.read_index_value(page, cur_offset).unwrap();
        let index = self.find_value_position(parent_page, &v).unwrap() as i32;
        let offset = self.get_cell_offset(parent_page, index as u16);
        let (_, _, parent_row_ids) = self.read_full_index_value(parent_page, offset);
        let num_cells = self.get_num_cells(parent_page);
        match direction {
            ..=-1 => {
                let new_row_ids = self.traverse(parent_page, 0, index - 1, direction);
                row_ids.extend(new_row_ids);
                row_ids.extend(parent_row_ids);
            }
            0 => {
                let new_row_ids =
                    self.traverse(parent_page, index + 1, num_cells as i32 - 1, direction);
                row_ids.extend(new_row_ids);
            }
            1.. => {
                let new_row_ids =
                    self.traverse(parent_page, index + 1, num_cells as i32 - 1, direction);
                row_ids.extend(new_row_ids);
            }
        }
        row_ids
    }

    fn search(&mut self, value: &DataType, operator: &str) -> Vec<u32> {
        let pos = self.find_value(value);
        let (page, index, exists) = match pos {
            Ok((p, i)) => (p, i, true),
            Err((p, i)) => (p, i, false),
        };
        let offset = self.get_cell_offset(page, index);
        let (_, _, cell_row_ids) = self.read_full_index_value(page, offset);
        let num_cells = self.get_num_cells(page) as i32;

        let index = index as i32;
        match operator {
            "=" => exists.then_some(cell_row_ids).unwrap_or_default(),
            "<>" => {
                let mut temp = self.traverse(page, 0, index - 1, -1);
                temp.extend(self.traverse(page, index + 1, num_cells - 1, 1));
                temp
            }
            "<" => self.traverse(page, 0, index - 1, -1),
            "<=" => self.traverse(page, 0, index, -1),
            ">" => self.traverse(page, index + 1, num_cells - 1, 1),
            ">=" => self.traverse(page, index, num_cells - 1, 1),
            _ => unreachable!("Invalid operator"),
        }
    }

    /// Calculates the size of the payload for an index value.
    fn payload_size(&self, value: &DataType, num_row_ids: usize) -> u16 {
        2 + value.size() + num_row_ids as u16 * 4
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
        println!("Page: {}", page);
        println!(
            "{:8}  {}, XX, {}, {}, {}, {}",
            "",
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
            for offset in offsets {
                self.print_cell(page, offset);
                println!();
            }
        }
    }

    pub fn print_cell(&mut self, page: u32, offset: u16) {
        let total_offset = page as u64 * PAGE_SIZE + offset as u64;
        print!("{:08X}  ", total_offset);
        let (value, child_page, row_ids) = self.read_full_index_value(page, offset);
        self.seek_to_page_offset(page, offset + 2);
        if let Some(child_page) = child_page {
            print!("{:08X} ", child_page);
            self.skip_bytes(4);
        }
        let payload_size = self.read_u16();
        print!("{:04X} ", payload_size.blue());
        if !row_ids.is_empty() {
            print!("{:?} ", row_ids.len().yellow());
        }
        if let Some(value) = value {
            print!("{:?} ", Into::<u8>::into(&value).cyan());
            print!("{:?} ", value.red());
        }
        for (i, id) in row_ids.iter().enumerate() {
            print!("{} ", rainbow(&format!("{:08X}", id), i));
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{remove_file, File},
        io::BufRead,
    };

    use crate::{
        constants::DataType,
        record::Record,
        table::{Column, Table},
        table_file::TableFile,
    };

    use super::*;

    fn setup(test_name: &str, test_file_path: &str, col_index: usize) -> IndexFile {
        let file_name = format!("data/{}.tbl", test_name);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test table file");
        }
        let _table_file = TableFile::new(test_name, "data");
        let records = setup_records(test_file_path);
        let column_names = (0..4)
            .map(|i| format!("column_{}", i))
            .collect::<Vec<String>>();
        let nullable = vec![true; 4];
        let columns = records[0]
            .values
            .clone()
            .into_iter()
            .zip(column_names)
            .zip(nullable)
            .map(|((data_type, name), nullable)| Column {
                name,
                data_type,
                nullable,
            })
            .collect::<Vec<Column>>();
        let col_name = columns[col_index].name.clone();
        let table = Table::new(test_name.to_string(), columns, "data".to_string());
        let file_name = format!("data/{}.{}.ndx", test_name, col_name);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test index file");
        }

        IndexFile::new(&table, col_index, "data")
    }

    fn setup_records(test_file_path: &str) -> Vec<Record> {
        let columns = vec![
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
        records
    }

    fn setup_index_file(test_index_name: &str) -> IndexFile {
        let mut index_file = setup(test_index_name, "data/testdata.txt", 1);
        let records = setup_records("data/testdata.txt");
        index_file.initialize_index(records);
        index_file
    }

    fn teardown(test_index_name: &str, col_index: usize) {
        let file_name = format!("data/{}.tbl", test_index_name);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test table file");
        }
        let file_name = format!("data/{}.column_{}.ndx", test_index_name, col_index);
        if std::path::Path::new(&file_name).exists() {
            remove_file(file_name).expect("Error removing test index file");
        }
    }

    fn teardown_index_file(test_index_name: &str) {
        teardown(test_index_name, 1);
    }

    #[test]
    fn test_initialize_index() {
        let col_index = 1;
        let mut index_file = setup("test_initialize_index", "data/testdata.txt", col_index);
        let records = setup_records("data/testdata.txt");
        index_file.initialize_index(records);
        index_file.print();
        teardown("test_initialize_index", col_index);
    }

    #[test]
    fn test_index_update() {
        let mut index_file = setup_index_file("test_index_update");
        index_file.update_record(
            9,
            &DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string()),
            &DataType::Text("2345 Test St, Testville, Testland, 12345".to_string()),
        );
        let search_result = index_file.search(
            &DataType::Text("2345 Test St, Testville, Testland, 12345".to_string()),
            "=",
        );
        assert_eq!(1, search_result.len());
        assert_eq!(9, search_result[0]);
        let search_result = index_file.search(
            &DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string()),
            "=",
        );
        assert_eq!(0, search_result.len());
        teardown_index_file("test_index_update");
    }

    #[test]
    fn test_index_remove_item_from_cell() {
        let mut index_file = setup_index_file("test_index_remove_item_from_cell");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        index_file.remove_item_from_cell(9, &value);
        let search_result = index_file.search(&value, "=");
        assert_eq!(0, search_result.len());
        teardown_index_file("test_index_remove_item_from_cell");
        let mut index_file = setup_index_file("test_index_remove_item_from_cell");
        index_file.insert_item_into_cell(10, &value);
        index_file.remove_item_from_cell(9, &value);
        let search_result = index_file.search(&value, "=");
        assert_eq!(1, search_result.len());
        assert_eq!(10, search_result[0]);
        teardown_index_file("test_index_remove_item_from_cell");
    }

    #[test]
    fn test_index_add_item_to_cell() {
        let mut index_file = setup_index_file("test_index_add_item_to_cell");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        index_file.insert_item_into_cell(10, &value);
        index_file.insert_item_into_cell(11, &value);
        let search_result = index_file.search(&value, "=");
        assert_eq!(3, search_result.len());
        assert_eq!(9, search_result[0]);
        assert_eq!(10, search_result[1]);
        assert_eq!(11, search_result[2]);
        teardown_index_file("test_index_add_item_to_cell");
    }

    #[test]
    fn test_index_find_value() {
        let mut index_file = setup_index_file("test_index_find_value");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        let (page, index) = index_file.find_value(&value).unwrap();
        assert_eq!(2, page);
        assert_eq!(3, index);
        let value =
            DataType::Text("ZEndOfAlphabet St 176, Summerholm, Guadeloupe, 673843".to_string());
        if index_file.find_value(&value).is_ok() {
            panic!("Value should not exist");
        } else if let Err((page, index)) = index_file.find_value(&value) {
            assert_eq!(2, page);
            assert_eq!(4, index);
        }
        teardown_index_file("test_index_find_value");
    }

    #[test]
    fn test_index_search() {
        let mut index_file = setup_index_file("test_index_search");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        let search_result = index_file.search(&value, "=");
        assert_eq!(1, search_result.len());
        assert_eq!(9, search_result[0]);
        let search_result = index_file.search(&value, ">");
        assert_eq!(1, search_result.len());
        assert_eq!(5, search_result[0]);
        let search_result = index_file.search(&value, "<");
        assert_eq!(8, search_result.len());
        let search_result = index_file.search(&value, ">=");
        assert_eq!(2, search_result.len());
        let search_result = index_file.search(&value, "<=");
        assert_eq!(9, search_result.len());
        let search_result = index_file.search(&value, "<>");
        assert_eq!(9, search_result.len());
        let non_existent_value =
            DataType::Text("ZEndOfAlphabet St 176, Summerholm, Guadeloupe, 673843".to_string());
        let search_result = index_file.search(&non_existent_value, "=");
        assert_eq!(0, search_result.len());
        teardown_index_file("test_index_search");
    }

    #[test]
    fn test_index_long_file() {
        let mut index_file = setup("test_index_long_file", "data/longdata.txt", 2);
        let records = setup_records("data/longdata.txt");
        index_file.initialize_index(records);
        teardown("test_index_long_file", 2);
    }
}
