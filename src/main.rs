use dump_file::DumpFile;

pub mod constants;
// pub mod table;
pub mod record;
pub mod index_file;
pub mod database_file;
pub mod table_file;
pub mod utils;
pub mod dump_file;
pub mod read_write_types;
pub mod table;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file>", args[0]);
        std::process::exit(1);
    }
    let file = std::path::Path::new(&args[1]);
    if !file.exists() {
        eprintln!("File not found: {}", file.display());
        std::process::exit(1);
    } else if file.is_dir() {
        eprintln!("File is a directory: {}", file.display());
        std::process::exit(1);
    } else if file.extension().is_none() {
        eprintln!("File has no extension: {}", file.display());
        std::process::exit(1);
    } else if file.extension().unwrap() != "tbl" {
        eprintln!("File is not a .tbl file: {}", file.display());
        std::process::exit(1);
    }
    let file_name = file.file_name().unwrap().to_str().unwrap().split('.').next().unwrap();
    let dir = std::path::Path::new(file).parent().unwrap().to_str().unwrap();
    let mut table = table_file::TableFile::new(file_name, dir);
    if args.len() == 2 {
        table.dump();
        table.print();
    } else if args.len() == 3 {
        let page_num = u32::from_str_radix(&args[2], 16).unwrap();
        table.dump_page(page_num);
        table.print_page(page_num);
    }
}
