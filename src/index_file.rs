#![allow(dead_code)]
#![allow(unused_variables)]
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
};
/// Indexfile page structure:
/// Header: 0x00-0x0F
///     * 0x00-0x00: Page type
///     * 0x01-0x01: Unused space
///     * 0x02-0x03: Number of cells
///     * 0x04-0x05: Start of content area
///     * 0x06-0x09: Rightmost child page
///     * 0x0A-0x0D: Parent page
///     * 0x0E-0x0F: Unused
/// Cell offsets: 0x10-0x1F
///     * 2-byte offsets to the start of each cell
/// Cells:
///     * Child page: 4-bytes, pointer to the child page, interior index pages only
///     * Payload size: 2-bytes, size in bytes of cell excluding child page and payload size
///     * Number of row IDs: 1-byte, number of row IDs
///     * Payload type: 1-byte, data type of the payload
///     * value: Variable length, the value of the cell
///     * Row IDs: 4-bytes each, row IDs with the value

pub struct IndexFile {
    file: File,
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
    pub fn new(table_name: &str, column_name: &str, dir: &str) -> Self {
        let path = format!("{}/{}.{}.ndx", dir, table_name, column_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.clone())
            .expect("Error opening index file");
        let mut idx = Self { file };
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

    pub fn read_full_index_value_index(
        &mut self,
        page: u32,
        index: u16,
    ) -> (Option<DataType>, Option<u32>, Vec<u32>) {
        let offset = self.get_cell_offset(page, index);
        self.read_full_index_value(page, offset)
    }

    pub fn create_interior_page(&mut self, parent: u32, child: u32) -> u32 {
        let page = self.create_page(parent, PageType::IndexInterior);
        self.set_rightmost_child(page, child);
        page
    }

    pub fn split_page(&mut self, page: u32, split_value: &DataType) -> u32 {
        let page_type = self.get_page_type(page);
        let mut parent_page = self.get_parent_page(page);
        if parent_page == 0xFFFFFFFF {
            parent_page = self.create_interior_page(parent_page, page);
            // Set the parent page
            self.update_parent_page(page, parent_page);
        }

        let middle = self.get_num_cells(page) / 2;
        let middle_offset = self.get_cell_offset(page, middle);
        let (value, child_pointer, row_ids) = self.read_full_index_value(page, middle_offset);

        // Create a new sibling page
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
        let cell_size = self.cell_size(&value, num_row_ids, page_type);
        let zero_bytes = vec![0; cell_size as usize];
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");

        self.move_cells(page, new_page, middle);
        if page_type == PageType::IndexInterior {
            self.update_parent_pages(new_page);
        }

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

    fn update_parent_pages(&mut self, new_parent: u32) {
        let num_cells = self.get_num_cells(new_parent);
        for i in 0..num_cells {
            let offset = self.get_cell_offset(new_parent, i);
            let (_, child, _) = self.read_full_index_value(new_parent, offset);
            if let Some(child) = child {
                self.update_parent_page(child, new_parent);
            }
        }
    }

    fn update_parent_page(&mut self, page: u32, parent: u32) {
        self.seek_to_page_offset(page, 0x0A);
        self.write_u32(parent);
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
        let zero_bytes = vec![0; (cell_offset - content_start) as usize];
        self.seek_to_page_offset(source_page, content_start);
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
        let dest_num_cells = self.get_num_cells(destination_page);
        self.seek_to_page_offset(destination_page, 0x10 + dest_num_cells * 2);
        let modified_offsets = cell_offsets
            .chunks(2)
            .map(|offset| (u16::from_le_bytes([offset[0], offset[1]]) as i32 - offset_diff as i32))
            .flat_map(|offset| (offset as u16).to_le_bytes().to_vec())
            .collect::<Vec<u8>>();
        self.write_all(&modified_offsets)
            .expect("Error writing modified offsets");

        // Update the number of cells on the destination page
        self.seek_to_page_offset(destination_page, 0x02);
        self.write_u16(num_cells_to_move + dest_num_cells);
        // Update the number of cells on the source page
        self.seek_to_page_offset(source_page, 0x02);
        self.write_u16(preceding_cell);

        // Fill the moved offsets with zero bytes
        let zero_bytes = vec![0; cell_offsets.len()];
        self.seek_to_page_offset(source_page, offset_location);
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");
    }

    pub fn initialize_index(&mut self, values: Vec<Record>, column_index: usize) {
        let mut values_and_ids: Vec<(DataType, Vec<u32>)> =
            values.iter().fold(Vec::new(), |mut acc, r| {
                let id = r.row_id;
                let value = &r.values[column_index];
                let idx = acc.iter().position(|(v, _)| v == value);
                if let Some(idx) = idx {
                    acc[idx].1.push(id);
                } else {
                    acc.push((value.clone(), vec![id]));
                }
                acc
            });
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
        let cell_size = self.cell_size(value, row_ids.len(), page_type);

        let page = if self.should_split(page, cell_size as i32) {
            let page = self.split_page(page, value);
            if child_page != 0xFFFFFFFF {
                self.update_parent_page(child_page, page);
            }
            page
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

    pub fn update_record(&mut self, row_id: u32, old_value: &DataType, new_value: &DataType) {
        self.remove_item_from_cell(row_id, old_value);
        self.insert_item_into_cell(row_id, new_value);
    }

    pub fn remove_item_from_cell(&mut self, row_id: u32, value: &DataType) {
        let (page, index) = self
            .find_value(value)
            .unwrap_or_else(|(p, _)| panic!("Value not found: {}", value));
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
        self.shift_cells(page, index as i32 - 1, -4, 0);

        if row_ids.is_empty() {
            self.remove_cell(page, index, true);
        }
    }

    fn remove_cell(&mut self, page: u32, index: u16, steal: bool) {
        let page_type = self.get_page_type(page);
        let offset = self.get_cell_offset(page, index);
        let (value, child_page, _) = self.read_full_index_value(page, offset);
        self.seek_to_page_offset(page, offset);
        if page_type == PageType::IndexInterior {
            self.skip_bytes(4);
        }
        let cell_size = (self.read_u16()
            + 2
            + match page_type {
                PageType::IndexInterior => 4,
                _ => 0,
            }) as i32;

        self.shift_cells(page, index as i32 - 1, -cell_size, -1);
        let num_cells = self.get_num_cells(page) - 1;
        self.seek_to_page_offset(page, 0x02); // Number of cells
        self.write_u16(num_cells);
        let content_start = self.get_content_start(page);
        let cell_pointer_list_end = 0x10 + num_cells * 2;
        self.seek_to_page_offset(page, cell_pointer_list_end);
        let zero_bytes = vec![0; (content_start - cell_pointer_list_end) as usize];
        self.write_all(&zero_bytes)
            .expect("Error writing zero bytes");

        if page_type == PageType::IndexInterior && steal {
            let (_, child_to_steal_from, _) = self.read_full_index_value_index(page, index - 1);
            self.steal_from_child(page, child_to_steal_from.unwrap(), child_page.unwrap());
        } else if page_type == PageType::IndexLeaf && num_cells == 0 {
            self.remove_page(page, None);
        }
    }

    fn remove_page(&mut self, page: u32, child: Option<u32>) {
        let parent_page = self.get_parent_page(page);
        if parent_page == 0xFFFFFFFF && child.is_none() {
            let rightmost_child = self.get_rightmost_child(page);
            self.seek_to_page_offset(rightmost_child, 0x0A);
            self.write_u32(0xFFFFFFFF);
            self.delete_page(page);
            return;
        }
        let index = self.find_page_pointer_index(parent_page, page);
        if let Some(right) = self.right_sibling(page) {
            match self.get_num_cells(right) {
                0 => panic!(
                    "Right sibling ({}) of {} has no cells: {}",
                    right,
                    page,
                    self.get_num_cells(right)
                ),
                2.. => self.steal_from_sibling(page, right, true),
                1 => {
                    self.merge_pages(parent_page, right, page, true);
                    self.set_rightmost_child(parent_page, right);
                    self.delete_page(page);
                    if self.get_num_cells(parent_page) == 1 {
                        self.remove_page(parent_page, None);
                    }
                }
            }
        } else if let Some(left) = self.left_sibling(page) {
            match self.get_num_cells(left) {
                0 => panic!(
                    "Left sibling ({}) of {} has no cells: {}",
                    left,
                    page,
                    self.get_num_cells(left)
                ),
                2.. => self.steal_from_sibling(page, left, false),
                1 => {
                    self.merge_pages(parent_page, left, page, false);
                    self.delete_page(page);
                    if self.get_num_cells(parent_page) == 1 {
                        self.remove_page(parent_page, None);
                    }
                }
            }
        } else {
            panic!(
                "Page has no siblings: {} {}",
                page,
                self.get_num_cells(page)
            );
        }
    }

    fn delete_page(&mut self, page: u32) {
        if self.len() == (page + 1) as u64 * PAGE_SIZE {
            self.set_len(self.len() - PAGE_SIZE);
            return;
        }
        let num_pages = (self.len() / PAGE_SIZE) as u32;
        let mut last_page_bytes = vec![0; PAGE_SIZE as usize];
        self.update_page_number(num_pages - 1, page);
        self.seek_to_page_offset(num_pages - 1, 0);
        self.read_exact(&mut last_page_bytes)
            .expect("Error reading following bytes");
        self.seek_to_page_offset(page, 0);
        self.write_all(&last_page_bytes)
            .expect("Error writing zero bytes");
        self.set_len(self.len() - PAGE_SIZE);
    }

    fn update_page_number(&mut self, old_page: u32, new_page: u32) {
        let parent_page = self.get_parent_page(old_page);
        let index = self.find_page_pointer_index(parent_page, old_page);
        if index == 0 {
            self.set_rightmost_child(parent_page, new_page);
            return;
        }
        let offset = self.get_cell_offset(parent_page, index);
        self.seek_to_page_offset(parent_page, offset);
        self.write_u32(new_page);
    }

    fn right_sibling(&mut self, page: u32) -> Option<u32> {
        let parent_page = self.get_parent_page(page);
        let num_cells = self.get_num_cells(parent_page);
        let index = self.find_page_pointer_index(parent_page, page);
        if index < num_cells - 1 {
            let offset = self.get_cell_offset(parent_page, index + 1);
            let (_, child, _) = self.read_full_index_value(parent_page, offset);
            child
        } else {
            None
        }
    }

    fn left_sibling(&mut self, page: u32) -> Option<u32> {
        let parent_page = self.get_parent_page(page);
        let index = self.find_page_pointer_index(parent_page, page);
        if index > 0 {
            let offset = self.get_cell_offset(parent_page, index - 1);
            let (_, child, _) = self.read_full_index_value(parent_page, offset);
            child
        } else {
            None
        }
    }

    fn get_rightmost_child(&mut self, page: u32) -> u32 {
        self.seek_to_page_offset(page, 6);
        self.read_u32()
    }

    fn steal_from_sibling(&mut self, page: u32, sibling: u32, right: bool) {
        if self.get_page_type(page) != PageType::IndexLeaf {
            unimplemented!("Stealing from interior pages not yet implemented");
        }
        if self.get_num_cells(page) != 0 {
            panic!("Page must be empty to steal from sibling");
        }
        let parent_page = self.get_parent_page(page);
        let index = match right {
            false => self.find_page_pointer_index(parent_page, page),
            true => self.find_page_pointer_index(parent_page, sibling),
        };
        let (value, _, row_ids) = self.read_full_index_value_index(parent_page, index);
        let value = value.unwrap();
        self.remove_cell(parent_page, index, false);

        let num_cells = self.get_num_cells(sibling);
        let sibling_index = if right { 0 } else { num_cells - 1 };
        if num_cells < 2 {
            panic!(
                "Sibling {} has too few cells {} to steal from",
                sibling, num_cells
            );
        }
        let (sibling_value, _, sibling_row_ids) =
            self.read_full_index_value_index(sibling, sibling_index);
        let sibling_value = sibling_value.unwrap();
        self.remove_cell(sibling, sibling_index, false);

        self.write_cell(page, &value, row_ids, 0xFFFFFFFF);
        if right {
            self.write_cell(parent_page, &sibling_value, sibling_row_ids, sibling);
        } else {
            self.write_cell(parent_page, &sibling_value, sibling_row_ids, page);
        }
    }

    fn steal_from_child(&mut self, parent: u32, child_to_steal_from: u32, child: u32) {
        let num_cells = self.get_num_cells(child_to_steal_from);
        if num_cells < 1 {
            panic!("Child page must have at least one cell to steal");
        }
        let index = num_cells - 1;
        let offset = self.get_cell_offset(child_to_steal_from, index);
        let (value, _, row_ids) = self.read_full_index_value(child_to_steal_from, offset);
        let value = value.unwrap();
        self.write_cell(parent, &value, row_ids, child);
        self.remove_cell(child_to_steal_from, index, true);
    }

    fn set_rightmost_child(&mut self, page: u32, child: u32) {
        if self.get_num_cells(page) == 0 {
            self.seek_to_page_offset(page, 0x02);
            self.write_u16(1); // Set the number of cells to 1
            self.write_u16(PAGE_SIZE as u16 - 6); // Set the start of the content area
            self.seek_to_page_offset(page, 0x10);
            self.write_u16(PAGE_SIZE as u16 - 6); // Set the offset of the first cell
        }
        self.seek_to_page_offset(page, PAGE_SIZE as u16 - 6);
        self.write_u32(child);
        self.seek_to_page_offset(page, 0x6);
        self.write_u32(child);
    }

    fn merge_pages(&mut self, parent: u32, merge: u32, deleted: u32, right: bool) {
        if self.get_num_cells(merge) != 1 {
            panic!(
                "Child page must have only one cell to merge, found: {}",
                self.get_num_cells(merge)
            );
        }
        let index = match right {
            true => self.find_page_pointer_index(parent, merge),
            false => self.find_page_pointer_index(parent, deleted),
        };
        let (value, _, row_ids) = self.read_full_index_value_index(parent, index);
        let value = value.unwrap();
        self.remove_cell(parent, index, false);
        self.write_cell(merge, &value, row_ids, 0xFFFFFFFF);
    }

    fn find_page_pointer_index(&mut self, page: u32, child_page: u32) -> u16 {
        let num_cells = self.get_num_cells(page);
        for i in 0..num_cells {
            let offset = self.get_cell_offset(page, i);
            let (_, child, _) = self.read_full_index_value(page, offset);
            if let Some(child) = child {
                if child == child_page {
                    return i;
                }
            }
        }
        panic!("Child page not found: {}", child_page);
    }

    pub fn insert_item_into_cell(&mut self, row_id: u32, new_value: &DataType) {
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

    pub fn search(&mut self, value: &DataType, operator: &str) -> Vec<u32> {
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

    fn cell_size(&self, value: &DataType, num_row_ids: usize, page_type: PageType) -> u16 {
        self.payload_size(value, num_row_ids)
            + 2
            + match page_type {
                PageType::IndexInterior => 4,
                _ => 0,
            }
    }

    pub fn print(&mut self) {
        let num_pages = self.len() / PAGE_SIZE;
        if num_pages == 0 {
            println!("Empty index file");
            return;
        }
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
        println!("Page: {:02X}", page);
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
        self.seek_to_page_offset(page, offset);
        if let Some(child_page) = child_page {
            print!("{:08X} ", child_page.green());
            self.skip_bytes(4);
        }
        let payload_size = self.read_u16();
        print!("{:04X} ", payload_size.blue());
        print!("{:?} ", row_ids.len().yellow());
        if let Some(value) = value {
            let v_u8 = Into::<u8>::into(&value);
            print!("{:02X}", v_u8.cyan());
            let v_size = value.size();
            print!("({}) ", v_size.cyan());
            print!("{:?} ", value.red());
        }
        print!("[ ");
        for (i, id) in row_ids.iter().enumerate() {
            print!("{}", rainbow(&format!("{:08X}", id), i));
            if i < row_ids.len() - 1 {
                print!(", ");
            }
        }
        print!(" ]");
    }
}

#[cfg(test)]
mod test {
    use crate::constants::DataType;
    use crate::utils::{setup_records, setup_table, teardown};

    use super::*;

    fn setup_index_file(test_name: &str) -> IndexFile {
        setup(test_name, 1, "data/testdata.txt")
    }

    fn setup(test_name: &str, col_index: usize, test_data: &str) -> IndexFile {
        let table = setup_table(test_name, test_data);
        let mut index_file = IndexFile::new(test_name, &table.columns[col_index].name, "data/test");
        index_file.initialize_index(setup_records(test_data), col_index);
        index_file
    }

    fn setup_uninitialized(test_name: &str, col_index: usize) -> IndexFile {
        let table = setup_table(test_name, "data/testdata.txt");
        IndexFile::new(test_name, &table.columns[col_index].name, "data/test")
    }

    #[test]
    fn test_initialize_index() {
        let col_index = 1;
        let mut index_file = setup_uninitialized("test_initialize_index", col_index);
        let records = setup_records("data/testdata.txt");
        index_file.initialize_index(records, col_index);
        teardown("test_initialize_index");
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
        teardown("test_index_update");
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
        teardown("test_index_add_item_to_cell");
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
        teardown("test_index_find_value");
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
        teardown("test_index_search");
    }

    #[test]
    fn test_index_long_file() {
        let index_file = setup("test_index_long_file", 2, "data/longdata.txt");
        assert_ne!(10, index_file.len() / PAGE_SIZE);
        teardown("test_index_long_file");
    }

    #[test]
    fn test_index_remove_item_from_cell() {
        let mut index_file = setup_index_file("test_index_remove_item_from_cell");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        index_file.remove_item_from_cell(9, &value);
        let search_result = index_file.search(&value, "=");
        assert_eq!(0, search_result.len());
        teardown("test_index_remove_item_from_cell");
        let mut index_file = setup_index_file("test_index_remove_item_from_cell");
        index_file.insert_item_into_cell(10, &value);
        index_file.remove_item_from_cell(9, &value);
        let search_result = index_file.search(&value, "=");
        assert_eq!(1, search_result.len());
        assert_eq!(10, search_result[0]);
        teardown("test_index_remove_item_from_cell");
    }

    #[test]
    fn test_index_remove_cell() {
        let mut index_file = setup_index_file("test_index_remove_cell");
        let value =
            DataType::Text("Terminology St 176, Summerholm, Guadeloupe, 673843".to_string());
        let (page, index) = index_file.find_value(&value).unwrap();
        let num_cells = index_file.get_num_cells(page);
        let (_, _, row_ids) = index_file.read_full_index_value_index(page, index);
        index_file.remove_item_from_cell(row_ids[0], &value);
        let new_num_cells = index_file.get_num_cells(page);
        assert_eq!(num_cells, new_num_cells + 1);
        let search_result = index_file.search(&value, "=");
        assert_eq!(0, search_result.len());
        teardown("test_index_remove_cell");
    }

    #[test]
    fn test_index_remove_last_cell_right() {
        let mut index_file = setup_index_file("test_index_remove_last_cell_right");
        let page = 2;
        for i in 0..8 {
            let offset = index_file.get_cell_offset(page, 0);
            let (value, _, row_ids) = index_file.read_full_index_value(page, offset);
            if let Some(value) = value {
                for id in row_ids {
                    index_file.remove_item_from_cell(id, &value);
                }
            }
        }
        assert_eq!(1, index_file.len() / PAGE_SIZE);
        assert_eq!(2, index_file.get_num_cells(0));
        teardown("test_index_remove_last_cell_right");
    }

    #[test]
    fn test_index_remove_last_cell_left() {
        let mut index_file = setup_index_file("test_index_remove_last_cell_left");
        let page = 0;
        for i in 0..8 {
            let offset = index_file.get_cell_offset(page, 0);
            let (value, _, row_ids) = index_file.read_full_index_value(page, offset);
            if let Some(value) = value {
                for id in row_ids {
                    index_file.remove_item_from_cell(id, &value);
                }
            }
        }
        assert_eq!(1, index_file.len() / PAGE_SIZE);
        assert_eq!(2, index_file.get_num_cells(0));
        teardown("test_index_remove_last_cell_left");
    }

    #[test]
    fn test_index_remove_last_cell_interior() {
        let mut index_file = setup_index_file("test_index_remove_last_cell_interior");
        let page = 1;
        for i in 0..8 {
            let offset = index_file.get_cell_offset(page, 1);
            let (value, _, row_ids) = index_file.read_full_index_value(page, offset);
            if let Some(value) = value {
                for id in row_ids {
                    index_file.remove_item_from_cell(id, &value);
                }
            }
        }
        assert_eq!(1, index_file.len() / PAGE_SIZE);
        assert_eq!(2, index_file.get_num_cells(0));
        teardown("test_index_remove_last_cell_interior");
    }
}
