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
    if std::env::args().len() == 2 {
        let file_path = std::env::args().nth(1).unwrap();
        let file_path = std::path::Path::new(&file_path);
        let file_dir = file_path.parent().unwrap().to_str().unwrap();
        let file_name = file_path.file_stem().unwrap().to_str().unwrap();
        let file_ext = file_path.extension().unwrap().to_str().unwrap();
        if file_ext == "tbl" {
            let mut table_file = table_file::TableFile::new(file_name, file_dir);
            table_file.dump();
            table_file.print();
        } else if file_ext == "ndx" {
            let column_name = file_name.split('.').nth(1).unwrap();
            let mut index_file = index_file::IndexFile::new(file_name, column_name, file_dir);
            index_file.print();
        } else {
            println!("Invalid file extension: {}", file_ext);
        }
        return;
    }
    let mut database = Database::new();
    database.run();
}
