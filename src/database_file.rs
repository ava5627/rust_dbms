use crate::constants::{PageType, PAGE_SIZE};
use crate::read_write_types::ReadWriteTypes;
use std::io::{Read, Seek, SeekFrom, Write};

pub trait DatabaseFile: Read + Write + Seek + ReadWriteTypes {
    fn set_len(&self, length: u64);
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn seek_to_page(&mut self, page: u32) -> u64 {
        self.seek(SeekFrom::Start(page as u64 * PAGE_SIZE))
            .expect("Failed to seek to page")
    }

    fn seek_to_page_offset(&mut self, page: u32, offset: u16) -> u64 {
        if offset as u64 >= PAGE_SIZE {
            panic!(
                "Offset {:02X} larger than page size ({:02X})",
                offset, PAGE_SIZE
            );
        }
        if page as u64 >= self.len() / PAGE_SIZE {
            panic!(
                "Page {:02X} out of bounds ({:02X})",
                page,
                self.len() / PAGE_SIZE - 1
            );
        }
        self.seek(SeekFrom::Start(page as u64 * PAGE_SIZE + offset as u64))
            .expect("Failed to seek to page offset")
    }

    fn skip_bytes(&mut self, bytes: i64) -> u64 {
        self.seek(SeekFrom::Current(bytes))
            .expect("Failed to skip bytes")
    }

    /// Creates a new page in the database file.
    ///
    /// * `parent_page` - The page number of the parent page.
    /// * `page_type` - The type of page to create.
    fn create_page(&mut self, parent_page: u32, page_type: PageType) -> u32 {
        let mut last_page = self.len() / PAGE_SIZE;
        for p in 0..self.len() / PAGE_SIZE {
            if self.get_page_type(p as u32) == PageType::Empty {
                last_page = p;
            }
        }
        self.set_len((last_page + 1) * PAGE_SIZE);
        self.seek(SeekFrom::Start(last_page * PAGE_SIZE))
            .expect("Failed to seek to new page");
        self.write_u8(page_type as u8); // page type 0x00
        self.write_u8(0x00); // unused 0x01
        self.write_u16(0x00); // number of cells 0x02-0x03
        self.write_u16(PAGE_SIZE as u16); // start of content area, set to end of page 0x04-0x05
        self.write_u32(0xFFFFFFFF); // rightmost child page if interior, right sibling if leaf 0x06-0x09
        self.write_u32(parent_page); // parent page 0x0A-0x0D
        self.write_u16(0x00); // unused 0x0E-0x0F
        last_page as u32
    }

    fn get_content_start(&mut self, page: u32) -> u16 {
        self.seek_to_page_offset(page, 0x04);
        self.read_u16()
    }

    fn get_parent_page(&mut self, page: u32) -> u32 {
        self.seek_to_page_offset(page, 0x0A);
        self.read_u32()
    }

    fn get_root_page(&mut self) -> u32 {
        let mut current_page = 0;
        while self.get_parent_page(current_page) != 0xFFFFFFFF {
            current_page = self.get_parent_page(current_page);
        }
        current_page
    }

    fn set_content_start(&mut self, page: u32, cell_size: i32) -> u16 {
        let old_content_start = self.get_content_start(page) as i32;
        let new_content_start = (old_content_start - cell_size) as u16;
        self.seek_to_page_offset(page, 0x04);
        self.write_u16(new_content_start);
        new_content_start
    }

    fn get_num_cells(&mut self, page: u32) -> u16 {
        self.seek_to_page_offset(page, 0x02);
        self.read_u16()
    }

    fn increment_num_cells(&mut self, page: u32) -> u16 {
        let old_num_cells = self.get_num_cells(page);
        let num_cells = old_num_cells + 1;
        self.seek_to_page_offset(page, 0x02);
        self.write_u16(num_cells);
        num_cells
    }

    /// Returns whether the page should be split after adding a new cell.
    fn should_split(&mut self, page: u32, cell_size: i32) -> bool {
        let num_cells = self.get_num_cells(page);
        let header_size = 0x10 + 2 * (num_cells + 1);
        let content_start = self.get_content_start(page);
        (content_start as i32 - cell_size) < (header_size as i32)
    }

    fn get_cell_offset(&mut self, page: u32, cell_num: u16) -> u16 {
        let offset = 0x10 + 2 * cell_num;
        self.seek_to_page_offset(page, offset);
        self.read_u16()
    }

    /// Returns the info for a page stored in its header.
    /// # Returns: tuple containing:
    ///    * page type: `PageType`
    ///    * number of cells: `u16`
    ///    * content start: `u16`
    ///    * rightmost child/neighboring page: `u32`
    ///    * parent page: `u32`
    fn get_page_info(&mut self, page: u32) -> (PageType, u16, u16, u32, u32) {
        self.seek_to_page(page);
        let page_type = self.read_u8().into();
        self.read_u8(); // unused
        let num_cells = self.read_u16();
        let content_start = self.read_u16();
        let rightmost_child = self.read_u32();
        let parent_page = self.read_u32();
        (
            page_type,
            num_cells,
            content_start,
            rightmost_child,
            parent_page,
        )
    }

    fn get_page_type(&mut self, page: u32) -> PageType {
        self.seek_to_page(page);
        self.read_u8().into()
    }

    /// Shifts all cells after `preceding_cell` on `page` towards the front of the page by `shift`
    /// bytes.
    ///
    /// * `page` - The page to shift cells on.
    /// * `preceding_cell` - The cell number of the cell before the first cell to shift.
    /// * `shift` - The number of bytes to shift the cells by.
    /// * `new_record_num` - number of records being added or removed. determines whether cell offsets should also be shifted.
    ///     * if positive, records are being added, shift following cell offsets forward by `new_record_num` bytes.
    ///     * if negative, records are being removed, shift following cell offsets backward by `new_record_num` bytes.
    ///     * if zero, no records are being added or removed. do not shift cell offsets.
    fn shift_cells(
        &mut self,
        page: u32,
        preceding_cell: i32,
        shift: i32,
        new_record_num: i32,
    ) -> u32 {
        if self.should_split(page, shift) {
            panic!("Shifting more than page can hold");
        }
        if preceding_cell == self.get_num_cells(page) as i32 - 1 {
            return self.set_content_start(page, shift) as u32;
        }

        let old_content_start = self.get_content_start(page);
        let content_offset = self.set_content_start(page, shift);

        if content_offset == PAGE_SIZE as u16 {
            return (PAGE_SIZE as i32 - shift) as u32;
        }

        let start_offset = if preceding_cell >= 0 {
            self.get_cell_offset(page, preceding_cell as u16)
        } else {
            PAGE_SIZE as u16
        };
        self.seek_to_page_offset(page, old_content_start);
        let mut bytes_to_shift = (start_offset - old_content_start) as i32;
        if shift < 0 {
            bytes_to_shift += shift;
        }
        let mut shifted_bytes: Vec<u8> = vec![0; bytes_to_shift as usize];
        self.read_exact(&mut shifted_bytes)
            .expect("Failed to read shifted bytes");

        self.seek_to_page_offset(page, content_offset);
        self.write_all(&shifted_bytes)
            .expect("Failed to write shifted bytes");

        let num_cells = self.get_num_cells(page);
        let num_shifted_cells = num_cells as i32 - preceding_cell - 1;

        let offset = 0x10 + 2 * (preceding_cell + 1);
        self.seek_to_page_offset(page, offset as u16);

        let mut cell_offsets: Vec<u8> = vec![0; 2 * (num_shifted_cells as usize)];
        self.read_exact(&mut cell_offsets)
            .expect("Failed to read cell offsets");

        let offset = 0x10 + 2 * (preceding_cell + new_record_num + 1);
        self.seek_to_page_offset(page, offset as u16);

        for i in (0..cell_offsets.len()).step_by(2) {
            let old_offset = u16::from_le_bytes([cell_offsets[i], cell_offsets[i + 1]]);
            self.write_u16((old_offset as i32 - shift) as u16);
        }

        let final_offset = start_offset as i32 - shift;
        final_offset as u32
    }

    fn delete_page(&mut self, page: u32) {
        self.seek_to_page(page);
        let empty_page = vec![0; PAGE_SIZE as usize];
        self.write_all(&empty_page).expect("Failed to delete page");
    }
}
