
pub mod constants;
// pub mod table;
pub mod record;
pub mod index_file;
pub mod database_file;
pub mod table_file;
pub mod utils;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file>", args[0]);
        std::process::exit(1);
    }
    let file = &args[1];
    let file_name = std::path::Path::new(file).file_name().unwrap().to_str().unwrap().split('.').next().unwrap();
    let dir = std::path::Path::new(file).parent().unwrap().to_str().unwrap();
    let mut table = table_file::TableFile::new(file_name, dir);
    table.dump().ok();
}
