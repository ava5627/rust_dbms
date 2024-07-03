pub mod constants;
pub mod database;
pub mod database_file;
pub mod dump_file;
pub mod index_file;
pub mod read_write_types;
pub mod record;
pub mod table;
pub mod table_file;
pub mod utils;

use database::Database;
use dump_file::DumpFile;

fn main() {
    if std::env::args().len() == 2 || std::env::args().len() == 3 {
        let file_path = std::env::args().nth(1).unwrap();
        let file_path = std::path::Path::new(&file_path);
        let file_dir = file_path.parent().unwrap().to_str().unwrap();
        let file_name = file_path.file_stem().unwrap().to_str().unwrap();
        let file_ext = file_path.extension().unwrap().to_str().unwrap();
        let page = if std::env::args().len() == 3 {
            Some(std::env::args().nth(2).unwrap().parse::<u32>().unwrap())
        } else {
            None
        };
        if file_ext == "tbl" {
            let mut table_file = table_file::TableFile::new(file_name, file_dir);
            if let Some(page) = page {
                table_file.dump_page(page);
                table_file.print_page(page);
            } else {
                table_file.dump();
                table_file.print();
            }
        } else if file_ext == "ndx" {
            let parts: Vec<&str> = file_name.split('.').collect();
            let table_name = parts[0];
            let column_name = parts[1];
            let mut index_file = index_file::IndexFile::new(table_name, column_name, file_dir);
            if let Some(page) = page {
                index_file.dump_page(page);
                index_file.print_page(page);
            } else {
                index_file.dump();
                index_file.print();
            }
        } else {
            println!("Invalid file extension: {}", file_ext);
        }
        return;
    }
    let mut database = Database::new();
    database.run();
}
