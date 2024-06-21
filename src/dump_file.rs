use std::io::Read;

use crate::constants::{DataType, PageType, PAGE_SIZE};
use crate::database_file::DatabaseFile;
use crate::index_file::IndexFile;
use crate::read_write_types::ReadWriteTypes;
use crate::table_file::TableFile;
use crate::utils::rainbow;
use owo_colors::OwoColorize;

pub trait DumpFile: DatabaseFile + ReadWriteTypes {
    fn dump(&mut self);
    fn dump_page(&mut self, page_num: u32);

    fn dump_bytes(&mut self, bytes: &[u8]) -> String {
        let mut bytes_str = String::new();
        for byte in bytes {
            bytes_str.push_str(&format!("{:02X} ", byte));
        }
        bytes_str
    }
}

impl DumpFile for TableFile {
    fn dump(&mut self) {
        let num_pages = self.len() / PAGE_SIZE;
        println!("num_pages: {}", num_pages);
        for i in 0..num_pages as u32 {
            self.dump_page(i);
            println!();
        }
    }

    fn dump_page(&mut self, page_num: u32) {
        let page_offset = page_num * PAGE_SIZE as u32;
        let bytes_per_row = 16;
        let num_rows = PAGE_SIZE as u32 / bytes_per_row;
        self.seek_to_page(page_num);
        print!("{:08X}  | ", page_offset);
        let page_type = self.read_u8();
        print!("{}", self.dump_bytes(&[page_type]).green());
        let unused = self.read_u8();
        print!("{}", self.dump_bytes(&[unused]));
        let num_cells = self.read_u16();
        print!("{}", self.dump_bytes(&num_cells.to_le_bytes()).blue());
        let content_start = self.read_u16();
        print!("{}", self.dump_bytes(&content_start.to_le_bytes()).purple());
        let next_page = self.read_u32();
        print!("{}", self.dump_bytes(&next_page.to_le_bytes()).yellow());
        let parent_page = self.read_u32();
        print!("{}", self.dump_bytes(&parent_page.to_le_bytes()).cyan());
        let unused = self.read_u16();
        print!("{}", self.dump_bytes(&unused.to_le_bytes()));
        print!(" | ");
        print!("{:?} ", PageType::from(page_type).green());
        print!("{} ", num_cells.blue());
        print!(
            "{:04X} {:04X} ",
            content_start.purple(),
            (content_start + page_offset as u16).purple()
        );
        print!("{:08X} ", next_page.yellow());
        print!("{:08X} ", parent_page.cyan());
        println!();
        let mut skip = false;
        let mut current_cell = 0;
        let mut offsets = vec![0; (num_cells * 2) as usize];
        let mut current_row: Option<(Vec<u8>, u8, u8, Vec<u8>)> = None;
        for i in 1..num_rows {
            let mut row_bytes = vec![0; bytes_per_row as usize];
            let mut pretty_row: Vec<String> = vec![];
            self.read_exact(&mut row_bytes)
                .expect("Failed to read row bytes");
            if row_bytes.iter().all(|&b| b == 0) {
                if !skip {
                    print!("{:08X}  | ", page_offset + i * bytes_per_row);
                    println!(" *");
                    skip = true;
                }
                continue;
            } else {
                skip = false;
            }
            print!("{:08X}  | ", page_offset + i * bytes_per_row);
            for (b, &byte) in row_bytes.iter().enumerate() {
                let byte_str = if current_cell / 2 < num_cells {
                    offsets[current_cell as usize] = byte;
                    current_cell += 1;
                    rainbow(
                        format!("{:02X} ", byte).as_str(),
                        (current_cell - 1) as usize / 2,
                    )
                } else if i * bytes_per_row + b as u32 >= content_start as u32
                    && PageType::from(page_type) == PageType::TableLeaf
                {
                    let offsets: Vec<u16> = offsets
                        .chunks(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    let current_offset = i * bytes_per_row + b as u32;
                    if offsets.contains(&(current_offset as u16)) && current_row.is_some() {
                        current_row = Some((vec![], 0, 0, vec![]));
                        pretty_row.push("Record starts before previous ends".on_red().to_string());
                    }
                    if let Some((row_bytes, col_num, col_index, ref cols)) = current_row {
                        let mut col_num = col_num;
                        let mut col_index = col_index;
                        let mut cols = cols.clone();
                        let current_row_index = row_bytes.len() as u8;
                        let mut row_bytes = row_bytes.clone();
                        row_bytes.push(byte);
                        let out = match current_row_index {
                            0 => format!("{:02X} ", byte.blue()),
                            1 => {
                                let row_size = u16::from_le_bytes([row_bytes[0], row_bytes[1]]);
                                pretty_row.push(format!("Row Size: {}", row_size.blue()));
                                format!("{:02X} ", byte.blue())
                            }
                            2..=4 => format!("{:02X} ", byte.yellow()),
                            5 => {
                                let row_id =
                                    u32::from_le_bytes(row_bytes[2..=5].try_into().unwrap());
                                pretty_row.push(format!("Row ID: {}", row_id.yellow()));
                                format!("{:02X} ", byte.yellow())
                            }
                            6 => {
                                let num_cols = byte as usize;
                                cols = vec![0; num_cols];
                                format!("{:02X} ", byte.red())
                            }
                            7.. => {
                                let num_cols = cols.len();
                                if current_row_index as usize - 7 < num_cols {
                                    let col_index = current_row_index as usize - 7;
                                    cols[col_index] = byte;
                                    rainbow(format!("{:02X} ", byte).as_str(), col_index)
                                } else if col_num < cols.len() as u8 {
                                    let col_size = DataType::size_type(cols[col_num as usize]);
                                    let out = rainbow(
                                        format!("{:02X} ", byte).as_str(),
                                        col_num as usize,
                                    )
                                    .on_truecolor(20, 20, 20)
                                    .to_string();
                                    if col_index == col_size - 1 {
                                        let col_bytes =
                                            &row_bytes[row_bytes.len() - col_size as usize..];
                                        let col_type = cols[col_num as usize];
                                        let col_value = match DataType::try_from((
                                            col_type,
                                            col_bytes.to_vec(),
                                        )) {
                                            Ok(value) => {
                                                rainbow(&format!("{:}", value), col_num as usize)
                                            }
                                            Err(_) => "Error parsing value".on_red().to_string(),
                                        };
                                        pretty_row.push(col_value);
                                        col_num += 1;
                                        col_index = 0;
                                    } else {
                                        col_index += 1;
                                    }
                                    out
                                } else {
                                    format!("{:02X} ", byte.on_red())
                                }
                            }
                        };
                        if col_num == cols.len() as u8 && current_row_index > 7 {
                            current_row = None;
                        } else {
                            current_row = Some((row_bytes, col_num, col_index, cols));
                        }
                        out
                    } else if offsets.contains(&(current_offset as u16)) {
                        current_row = Some((vec![byte], 0, 0, vec![]));
                        format!("{:02X} ", byte.blue())
                    } else {
                        format!("{:02X} ", byte.on_red().black())
                    }
                } else if i * bytes_per_row + b as u32 >= content_start as u32
                    && PageType::from(page_type) == PageType::TableInterior
                {
                    if let Some((ref row_bytes, _, _, _)) = current_row {
                        let current_row_index = row_bytes.len() as u8;
                        let mut row_bytes = row_bytes.clone();
                        row_bytes.push(byte);

                        current_row = Some((row_bytes.clone(), 0, 0, vec![]));
                        match current_row_index {
                            1..=2 => {
                                format!("{:02X} ", byte.blue())
                            }
                            3 => {
                                let row_id =
                                    u32::from_le_bytes(row_bytes[0..4].try_into().unwrap());
                                pretty_row.push(format!("(Child page: {:08X}", row_id.blue()));
                                format!("{:02X} ", byte.blue())
                            }
                            4..=6 => {
                                format!("{:02X} ", byte.yellow())
                            }
                            7 => {
                                let row_id =
                                    u32::from_le_bytes(row_bytes[4..8].try_into().unwrap());
                                pretty_row.push(format!("Row id: {:08X})", row_id.yellow()));
                                current_row = None;
                                format!("{:02X} ", byte.yellow())
                            }
                            _ => {
                                format!("{:02X} ", byte.on_red())
                            }
                        }
                    } else {
                        current_row = Some((vec![byte], 0, 0, vec![]));
                        format!("{:02X} ", byte.blue())
                    }
                } else {
                    format!("{:02X} ", byte)
                };
                print!("{}", byte_str);
            }
            println!(" | {}", pretty_row.join(" "));
        }
    }
}

impl DumpFile for IndexFile {
    fn dump(&mut self) {
        let num_pages = self.len() / PAGE_SIZE;
        for i in 0..num_pages as u32 {
            self.dump_page(i);
            println!();
        }
    }

    fn dump_page(&mut self, page_num: u32) {
        let page_offset = page_num * PAGE_SIZE as u32;
        let bytes_per_row = 16;
        let num_rows = PAGE_SIZE as u32 / bytes_per_row;
        self.seek_to_page(page_num);
        print!("{:08X}  | ", page_offset);
        let page_type = self.read_u8();
        print!("{}", self.dump_bytes(&[page_type]).green());
        let unused = self.read_u8();
        print!("{}", self.dump_bytes(&[unused]));
        let num_cells = self.read_u16();
        print!("{}", self.dump_bytes(&num_cells.to_le_bytes()).blue());
        let content_start = self.read_u16();
        print!("{}", self.dump_bytes(&content_start.to_le_bytes()).purple());
        let next_page = self.read_u32();
        print!("{}", self.dump_bytes(&next_page.to_le_bytes()).yellow());
        let parent_page = self.read_u32();
        print!("{}", self.dump_bytes(&parent_page.to_le_bytes()).cyan());
        let unused = self.read_u16();
        print!("{}", self.dump_bytes(&unused.to_le_bytes()));
        print!(" | ");
        print!("{:?} ", PageType::from(page_type).green());
        print!("{} ", num_cells.blue());
        print!(
            "{:04X} {:04X} ",
            content_start.purple(),
            (content_start + page_offset as u16).purple()
        );
        print!("{:08X} ", next_page.yellow());
        print!("{:08X} ", parent_page.cyan());
        println!();

        let mut skip = false;
        let mut current_cell = 0;
        let mut offsets = vec![0; (num_cells * 2) as usize];
        let mut current_row: Option<(Vec<u8>, usize, u8, u8)> = None;
        for i in 1..num_rows {
            let mut row_bytes = vec![0; bytes_per_row as usize];
            let mut pretty_row: Vec<String> = vec![];
            self.read_exact(&mut row_bytes)
                .expect("Failed to read row bytes");
            if row_bytes.iter().all(|&b| b == 0)
                && (i + 1) * bytes_per_row < content_start as u32
            {
                if !skip {
                    print!("{:08X}  | ", page_offset + i * bytes_per_row);
                    println!(" *");
                    skip = true;
                }
                continue;
            } else {
                skip = false;
            }
            print!("{:08X}  | ", page_offset + i * bytes_per_row);
            for (b, &byte) in row_bytes.iter().enumerate() {
                let byte_str = if current_cell / 2 < num_cells {
                    offsets[current_cell as usize] = byte;
                    current_cell += 1;
                    if current_cell % 2 == 0 {
                        let offset = u16::from_le_bytes([offsets[current_cell as usize - 2], byte]);
                        pretty_row.push(rainbow(
                            format!("{:04X}", offset).as_str(),
                            (current_cell - 1) as usize / 2,
                        ));
                    }
                    rainbow(
                        format!("{:02X} ", byte).as_str(),
                        (current_cell - 1) as usize / 2,
                    )
                } else if i * bytes_per_row + b as u32 >= content_start as u32 {
                    let offsets: Vec<u16> = offsets
                        .chunks(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    let current_offset = i * bytes_per_row + b as u32;
                    if offsets.contains(&(current_offset as u16)) && current_row.is_some() {
                        pretty_row.push("Record starts before previous ends".on_red().to_string());
                        current_row = Some((vec![], 0, 0, 0));
                    }
                    if let Some((ref row_bytes, val_index, val_type, num_row_ids)) = current_row {
                        let mut val_index = val_index;
                        let mut val_type = val_type;
                        let mut num_row_ids = num_row_ids;
                        let val_size = DataType::size_type(val_type);
                        let mut current_row_index = row_bytes.len() as u8;
                        if PageType::from(page_type) == PageType::IndexLeaf {
                            current_row_index += 4;
                        }
                        let mut row_bytes = row_bytes.clone();
                        row_bytes.push(byte);
                        let out = match current_row_index {
                            0..=2 => format!("{:02X} ", byte.on_blue()),
                            3 => {
                                let child_page =
                                    u32::from_le_bytes(row_bytes[0..4].try_into().unwrap());
                                pretty_row.push(format!("Child Page: {}", child_page));
                                format!("{:02X} ", byte.on_blue())
                            }
                            4 => format!("{:02X} ", byte.blue()),
                            5 => {
                                let o = match PageType::from(page_type) {
                                    PageType::IndexLeaf => (0, 2),
                                    PageType::IndexInterior => (4, 6),
                                    _ => unreachable!(),
                                };
                                let payload_size =
                                    u16::from_le_bytes(row_bytes[o.0..o.1].try_into().unwrap());
                                pretty_row
                                    .push(format!("Payload Size: {:02X}", payload_size.blue()));
                                format!("{:02X} ", byte.blue())
                            }
                            6 => {
                                num_row_ids = byte;
                                pretty_row.push(format!("Num Row ids: {}", num_row_ids.yellow()));
                                format!("{:02X} ", byte.yellow())
                            }
                            7 => {
                                val_type = byte;
                                let val_data_type = DataType::from(val_type);
                                pretty_row.push(format!("{:?}", val_data_type.cyan()));
                                format!("{:02X} ", byte.cyan())
                            }
                            8.. => {
                                if current_row_index < 7 + val_size {
                                    format!("{:02X} ", byte.red())
                                } else if current_row_index == 7 + val_size {
                                    let o = match PageType::from(page_type) {
                                        PageType::IndexLeaf => 4,
                                        PageType::IndexInterior => 8,
                                        _ => unreachable!(),
                                    };
                                    let value = match DataType::try_from((
                                        val_type,
                                        row_bytes[o..].to_vec(),
                                    )) {
                                        Ok(value) => format!("{:?}", value.red()),
                                        Err(_) => "Error parsing value".to_string(),
                                    };
                                    pretty_row.push(value);
                                    pretty_row.push("[".to_string());
                                    format!("{:02X} ", byte.red())
                                } else if current_row_index <= 7 + val_size + num_row_ids * 4 {
                                    if val_index == 0 {
                                        val_index = row_bytes.len();
                                    }
                                    if val_index == row_bytes.len() - 3 {
                                        let row_id = u32::from_le_bytes(
                                            row_bytes[val_index - 1..].try_into().unwrap(),
                                        );
                                        pretty_row.push(format!("{},", row_id.purple()));
                                        val_index = 0;
                                    }
                                    format!("{:02X} ", byte.purple())
                                } else {
                                    format!("{:02X} ", byte.on_red())
                                }
                            }
                        };
                        if current_row_index == 7 + val_size + num_row_ids * 4 && val_size > 0 {
                            pretty_row.push("]".to_string());
                            current_row = None;
                        } else {
                            current_row = Some((row_bytes, val_index, val_type, num_row_ids));
                        }
                        out
                    } else if offsets.contains(&(current_offset as u16)) {
                        current_row = Some((vec![byte], 0, 0, 0));
                        if PageType::from(page_type) == PageType::IndexLeaf {
                            format!("{:02X} ", byte.blue())
                        } else {
                            format!("{:02X} ", byte.on_blue())
                        }
                    } else {
                        format!("{:02X} ", byte.on_red().black())
                    }
                } else {
                    format!("{:02X} ", byte)
                };
                print!("{}", byte_str);
            }
            println!(" | {}", pretty_row.join(" "));
        }
    }
}
