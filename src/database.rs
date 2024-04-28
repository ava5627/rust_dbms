use std::fmt::Write as FmtWrite;
use std::{collections::VecDeque, io::Write};

use crate::{
    constants::*,
    record::Record,
    table::{Column, Table},
};

pub struct Database {
    pub table_table: Table,
    pub column_table: Table,
}

impl Database {
    pub fn new() -> Self {
        let system_dir = std::path::Path::new(SYSTEM_DIR);
        if !system_dir.exists() {
            Database::create_system_dir();
        }
        let text_type = DataType::Text(Default::default());
        let tiny_int_type = DataType::TinyInt(Default::default());
        let table_table_columns = vec![
            Column::new("table_name", text_type.clone(), false, true),
            Column::new("table_type", tiny_int_type.clone(), false, false),
        ];
        let mut table_table = Table::new(TABLE_TABLE, table_table_columns, SYSTEM_DIR);
        let column_table_columns = vec![
            Column::new("table_name", text_type.clone(), false, false),
            Column::new("column_name", text_type.clone(), false, false),
            Column::new("data_type", tiny_int_type.clone(), false, false),
            Column::new("ordinal_position", tiny_int_type.clone(), false, false),
            Column::new("is_nullable", tiny_int_type, false, false),
            Column::new("column_key", text_type, true, false),
        ];
        let mut column_table = Table::new(COLUMN_TABLE, column_table_columns, SYSTEM_DIR);
        Database::initialize_meta_tables(&mut table_table, &mut column_table)
            .expect("Failed initializing meta tables");
        Self {
            table_table,
            column_table,
        }
    }

    fn create_system_dir() {
        std::fs::create_dir_all(SYSTEM_DIR).expect("Failed creating system directory");
        std::fs::create_dir_all(USER_DIR).expect("Failed creating user directory");
    }

    fn initialize_meta_tables(
        table_table: &mut Table,
        column_table: &mut Table,
    ) -> Result<(), String> {
        if table_table.is_empty() {
            table_table.insert(vec![
                DataType::Text(TABLE_TABLE.to_string()),
                DataType::TinyInt(0),
            ])?;
            table_table.insert(vec![
                DataType::Text(COLUMN_TABLE.to_string()),
                DataType::TinyInt(0),
            ])?;
        }

        if column_table.is_empty() {
            column_table.insert(cols_vec(TABLE_TABLE, "table_name", 0x0C, 0, 0, "PRI"))?;
            column_table.insert(cols_vec(TABLE_TABLE, "table_type", 0x01, 1, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "table_name", 0x0C, 0, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "column_name", 0x01, 1, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "data_type", 0x01, 2, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "ordinal_position", 0x01, 3, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "is_nullable", 0x01, 4, 0, ""))?;
            column_table.insert(cols_vec(COLUMN_TABLE, "column_key", 0x0C, 5, 0, ""))?;
        }
        Ok(())
    }

    pub fn load_table(&mut self, table_name: &str) -> Option<Table> {
        let table_name_dt = DataType::Text(table_name.to_string());
        let tables = self
            .table_table
            .search(Some("table_name"), table_name_dt.clone(), "=")
            .unwrap();
        if tables.len() > 1 {
            panic!("Multiple tables with the same name found");
        }
        if tables.is_empty() {
            return None;
        }
        let mut columns = self
            .column_table
            .search(Some("table_name"), table_name_dt.clone(), "=")
            .unwrap();
        columns.sort_by(|a, b| {
            let a = match &a.values[3] {
                DataType::TinyInt(v) => *v,
                _ => unreachable!("Ordinal position should be tiny int"),
            };
            let b = match &b.values[3] {
                DataType::TinyInt(v) => *v,
                _ => unreachable!("Ordinal position should be tiny int"),
            };
            a.cmp(&b)
        });
        let columns = columns
            .iter()
            .map(|c| {
                let column_name = match &c.values[1] {
                    DataType::Text(v) => v,
                    _ => unreachable!("Column name should be text"),
                };
                let data_type: DataType = match &c.values[2] {
                    DataType::TinyInt(v) => (*v as u8).into(),
                    _ => unreachable!("Data type should be tiny int"),
                };
                let nullable = match &c.values[4] {
                    DataType::TinyInt(v) => *v,
                    _ => unreachable!("Nullable should be tiny int"),
                };
                let column_key = match &c.values[5] {
                    DataType::Text(v) => v,
                    DataType::Null => "",
                    _ => unreachable!("Column key should be text"),
                };
                let unique = column_key == "UNI";
                Column::new(column_name, data_type, nullable == 1, unique)
            })
            .collect::<Vec<Column>>();
        let table_type = match &tables[0].values[1] {
            DataType::TinyInt(v) => *v,
            _ => unreachable!("Table type should be tiny int"),
        };
        if table_type == 0 {
            Some(Table::new(table_name, columns, SYSTEM_DIR))
        } else if table_type == 1 {
            Some(Table::new(table_name, columns, USER_DIR))
        } else {
            panic!("Invalid table type: {}", table_type);
        }
    }

    pub fn new_table(
        &mut self,
        table: &mut Table,
        flags: Vec<(bool, bool, bool)>,
    ) -> Result<(), String> {
        self.table_table
            .insert(vec![
                DataType::Text(table.name.clone()),
                DataType::TinyInt(1),
            ])
            .expect("Failed inserting");

        let mut primary_key: Option<Column> = None;
        for (i, (column, flag)) in table.columns.iter().zip(flags.iter()).enumerate() {
            let nullable = if flag.1 { 0 } else { 1 };
            let column_key = if flag.0 {
                "PRI"
            } else if flag.2 {
                "UNI"
            } else {
                ""
            };
            let column_type = match column.data_type.clone() {
                DataType::Text(_) => 0x0C,
                v => Into::<u8>::into(&v),
            } as i8;
            self.column_table
                .insert(cols_vec(
                    table.name.as_str(),
                    column.name.as_str(),
                    column_type,
                    i as i8 + 1,
                    nullable,
                    column_key,
                ))
                .expect("Failed inserting");
            if column_key == "PRI" {
                if primary_key.is_some() {
                    return Err("Multiple primary keys not allowed".to_string());
                }
                primary_key = Some(column.clone());
            }
        }
        if let Some(primary_key) = primary_key {
            table.create_index(primary_key.name.as_str())?;
        }

        Ok(())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        let table_name_dt = DataType::Text(table_name.to_string());
        let table = self
            .load_table(table_name)
            .ok_or(format!("Table {} does not exist", table_name))?;
        let index_column = table.has_index();
        if let Some(index_column) = index_column {
            table.drop_index(index_column)?;
        }
        self.column_table
            .delete(Some("table_name"), &table_name_dt.clone(), "=")?;
        self.table_table
            .delete(Some("table_name"), &table_name_dt, "=")?;
        Ok(())
    }

    pub fn read_input(&self) -> String {
        // print prompt
        // read until ';'
        // return input
        let prompt = "db > ";
        print!("{}", prompt);
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        loop {
            std::io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");
            if input.ends_with(";\n") {
                break;
            }
            print!("{}", prompt);
        }
        input
    }

    pub fn run(&mut self) {
        println!("Welcome to RustDB!\n");
        loop {
            let input = self.read_input();
            println!("Command: {}", input);
            let res = self.parse_user_input(&input);
            match res {
                Ok(m) => println!("{}", m),
                Err(e) => println!("{}", e),
            }
        }
    }

    pub fn parse_user_input(&mut self, input: &str) -> Result<String, String> {
        let mut tokens: VecDeque<String> = input
            .replace('(', " ( ")
            .replace(')', " ) ")
            .replace(',', " , ")
            .replace(';', "")
            .split_whitespace()
            .map(|t| t.to_string().to_lowercase())
            .collect();
        match tokens.pop_front().as_deref().unwrap_or("") {
            "show" => self.show(&mut tokens),
            "select" => self.parse_select(&mut tokens),
            "create" => self.parse_create(&mut tokens),
            "insert" => self.parse_insert(&mut tokens),
            "update" => self.parse_update(&mut tokens),
            "delete" => self.parse_delete(&mut tokens),
            "drop" => self.parse_drop(&mut tokens),
            "help" => self.parse_help(&mut tokens),
            "exit" => std::process::exit(0),
            _ => Err(format!("Invalid command: {}", input)),
        }
    }

    fn show(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        if tokens.pop_front().as_deref() != Some("tables") {
            return Err("Invalid SHOW command. Expected TABLES.".to_string());
        }
        let tables = self.table_table.search(None, DataType::Null, "=")?;
        self.display(tables, &[0])
    }

    fn display(&self, records: Vec<Record>, columns: &[usize]) -> Result<String, String> {
        if records.is_empty() {
            return Ok("No records found.".to_string());
        }
        // Find the maximum width of each column
        let mut column_widths = vec![0; records[0].values.len()];
        for r in &records {
            let widths = r.column_widths();
            column_widths
                .iter_mut()
                .zip(widths.iter())
                .for_each(|(a, b)| *a = (*a).max(*b));
        }
        column_widths = column_widths
            .iter()
            .enumerate()
            .filter_map(|(i, w)| columns.contains(&i).then_some(*w))
            .collect();
        let mut out = String::new();
        for record in &records {
            let line = record.print_columns(columns, &column_widths);
            writeln!(&mut out, "{}", line).expect("Error writing to str");
        }
        Ok(out)
    }

    fn parse_select(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        let mut c_tokens: VecDeque<String> = tokens
            .iter()
            .take_while(|t| t.to_lowercase() != "from")
            .cloned()
            .collect();
        for _ in 0..c_tokens.len() {
            tokens.pop_front();
        }
        tokens.pop_front();
        let table = tokens.pop_front().ok_or("No table specified.")?;
        let mut table = self
            .load_table(&table)
            .ok_or(format!("Table {} not found.", table))?;
        let columns = self.parse_columns(&mut c_tokens, &table)?;
        let column_ids: Vec<usize> = columns
            .iter()
            .map(|c| table.columns.iter().position(|x| x.name == c.name).unwrap())
            .collect();
        let (search_column, operator, value) = self.parse_condition(tokens, &table)?;
        let mut out = format!("Table: {}\n", table.name);
        for c in &columns {
            write!(&mut out, "{} ", c.name).expect("Error writing to str");
        }
        writeln!(&mut out).expect("Error writing to str");
        let records = table.search(search_column.as_deref(), value, &operator)?;
        let records = self.display(records, &column_ids);
        write!(&mut out, "{}", records?).expect("Error writing to str");
        Ok(out)
    }

    fn parse_create(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        match tokens.pop_front().as_deref() {
            Some("table") => self.create_table(tokens),
            Some("index") => self.create_index(tokens),
            _ => Err("Invalid CREATE command. Expected TABLE or INDEX.".to_string()),
        }
    }

    fn create_table(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        let mut columns = vec![];
        let mut flag_vec = vec![];
        let table_name = tokens.pop_front().ok_or("No table name specified.")?;
        if self.load_table(&table_name).is_some() {
            return Err("Table already exists.".to_string());
        }
        if tokens.pop_front().as_deref() != Some("(") {
            return Err("Expected '('.".to_string());
        }
        loop {
            let column_name = tokens.pop_front().ok_or("No column name specified.")?;
            let data_type = tokens
                .pop_front()
                .ok_or("No data type specified.")?
                .parse()?;
            let mut flags = (false, false, false);
            let mut done = false;
            while let Some(next) = tokens.pop_front().as_deref() {
                match next {
                    "primary_key" => flags.0 = true,
                    "not_null" => flags.1 = true,
                    "unique" => flags.2 = true,
                    "," => break,
                    ")" => {
                        done = true;
                        break;
                    }
                    _ => return Err("Expected ',' or ')' or flag.".to_string()),
                }
            }
            flag_vec.push(flags);
            let col = Column::new(&column_name, data_type, !flags.1, flags.2);
            columns.push(col);
            if done {
                break;
            }
        }
        if flag_vec.iter().filter(|(p, _, _)| *p).count() > 1 {
            return Err("Multiple primary keys specified.".to_string());
        }
        if !tokens.is_empty() && tokens.pop_front().as_deref() != Some(";") {
            return Err("Unexpected tokens.".to_string());
        }
        let mut table = Table::new(&table_name, columns, USER_DIR);
        self.new_table(&mut table, flag_vec)?;
        Ok(format!("{} created.", table_name))
    }

    fn create_index(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        let table_name = tokens.pop_front().ok_or("No table name specified.")?;
        let mut table = self.load_table(&table_name).ok_or("Table not found.")?;
        let next = tokens.pop_front().ok_or("No column name specified.")?;
        let column = if next == "(" {
            let column = tokens.pop_front().ok_or("No column name specified.")?;
            tokens.pop_front().ok_or("Expected ')'")?;
            column
        } else {
            next
        };
        table.create_index(&column)?;
        Ok(format!("Index created on {}({}).", table_name, column))
    }

    fn parse_insert(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        if tokens.pop_front().ok_or("Expected INTO")? != "into" {
            return Err("Expected INTO.".to_string());
        }
        let table_name = tokens.pop_front().ok_or("No table specified.")?;
        let mut table = self.load_table(&table_name).ok_or("Table not found.")?;
        let columns = match tokens
            .pop_front()
            .ok_or("Expected columns or VALUES.")?
            .as_str()
        {
            "(" => self.parse_columns(tokens, &table)?,
            "values" => table.columns.iter().collect(),
            _ => return Err("Expected columns or VALUES.".to_string()),
        };
        let values = self.parse_values(tokens, &columns)?;
        if values.len() != columns.len() {
            return Err("Number of columns and values do not match.".to_string());
        }
        let mut col_val = columns.into_iter().zip(values);
        let full_valls = table
            .columns
            .iter()
            .map(|c| {
                col_val
                    .find(|(col, _)| col.name == c.name)
                    .unwrap_or((c, DataType::Null))
                    .1
                    .clone()
            })
            .collect();
        table.insert(full_valls)?;
        Ok(format!("1 row inserted into {}.", table_name))
    }

    fn parse_update(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        let table_name = tokens.pop_front().ok_or("No table specified.")?;
        let mut table = self.load_table(&table_name).ok_or("Table not found.")?;
        if tokens.pop_front().as_deref() != Some("set") {
            return Err("Expected SET.".to_string());
        }
        let column_name = tokens.pop_front().ok_or("No column specified.")?;
        let column = table
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .ok_or(format!("Column {} not found.", column_name))?;
        if tokens.pop_front().as_deref() != Some("=") {
            return Err("Expected =.".to_string());
        }
        let value_str = tokens.pop_front().ok_or("No value specified.")?;
        let value = DataType::parse_str(column.data_type.clone(), &value_str)?;
        let (search_column, search_operator, search_value) =
            self.parse_condition(tokens, &table)?;
        let updated = table.update(
            search_column.as_deref(),
            search_value,
            &search_operator,
            &column_name,
            value,
        )?;
        Ok(format!("{} rows updated.", updated))
    }

    fn parse_delete(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        if tokens.pop_front().as_deref() != Some("from") {
            return Err("Expected FROM.".to_string());
        }
        let table_name = tokens.pop_front().ok_or("No table specified.")?;
        let mut table = self.load_table(&table_name).ok_or("Table not found.")?;
        let (search_column, search_operator, search_value) =
            self.parse_condition(tokens, &table)?;
        let deleted = table.delete(search_column.as_deref(), &search_value, &search_operator)?;
        Ok(format!("{} rows deleted.", deleted))
    }

    fn parse_drop(&mut self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        if tokens.pop_front().as_deref() != Some("table") {
            return Err("Expected TABLE.".to_string());
        }
        let table_name = tokens.pop_front().ok_or("No table specified.")?;
        self.drop_table(&table_name)?;
        Ok(format!("Table {} dropped.", table_name))
    }

    fn parse_columns<'a>(
        &self,
        tokens: &mut VecDeque<String>,
        table: &'a Table,
    ) -> Result<Vec<&'a Column>, String> {
        if tokens[0] == "*" {
            tokens.pop_front();
            return Ok(table.columns.iter().collect());
        }

        let collect: Result<Vec<&Column>, String> = tokens
            .iter()
            .take_while(|&t| t.to_lowercase() != "from" && t != ")")
            .filter(|&t| t != ",")
            .map(|t| {
                table
                    .columns
                    .iter()
                    .find(|&c| c.name == *t)
                    .ok_or(format!("Column {} not found.", t))
            })
            .collect();
        while let Some(t) = tokens.pop_front() {
            if t == "from" || t == "values" {
                break;
            }
        }
        collect
    }

    fn parse_values(
        &self,
        tokens: &mut VecDeque<String>,
        columns: &[&Column],
    ) -> Result<Vec<DataType>, String> {
        if tokens.is_empty() {
            return Err("No values specified.".to_string());
        }
        if let Some(t) = tokens.pop_front() {
            if t != "(" {
                return Err(format!("Expected '('. Found: {}", t));
            }
        }
        let mut values = vec![];
        let mut current = String::new();
        while let Some(t) = tokens.pop_front() {
            if t == "," || t == ")" {
                if columns.len() < values.len() + 1 {
                    return Err(format!(
                        "Too many values specified. Expected {}.",
                        columns.len()
                    ));
                }
                values.push(DataType::parse_str(
                    columns[values.len()].data_type.clone(),
                    &current,
                )?);
                current.clear();
                continue;
            }
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(&t);
        }
        Ok(values)
    }

    fn parse_condition(
        &self,
        tokens: &mut VecDeque<String>,
        table: &Table,
    ) -> Result<(Option<String>, String, DataType), String> {
        if tokens.pop_front().as_deref() != Some("where") {
            return Ok((None, "=".to_string(), DataType::Null));
        }
        let column_name = tokens.pop_front().ok_or("No column specified.")?;
        let column = table
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .ok_or(format!("Column {} not found.", column_name))?;
        let operator = tokens.pop_front().ok_or("No operator specified.")?;
        let value_str = tokens.pop_front().ok_or("No value specified.")?;
        let value = DataType::parse_str(column.data_type.clone(), &value_str)?;
        Ok((Some(column_name), operator, value))
    }

    fn parse_help(&self, tokens: &mut VecDeque<String>) -> Result<String, String> {
        if tokens.len() > 1 {
            return Err("Invalid HELP command. Expected no arguments.".to_string());
        }
        let mut out = String::new();
        writeln!(&mut out, "Available commands:").expect("Error writing to str");
        writeln!(&mut out, "SHOW TABLES;").expect("Error writing to str");
        writeln!(&mut out, "\tDisplay a list of all tables in the database.")
            .expect("Error writing to str");
        writeln!(
            &mut out,
            "SELECT <columns> FROM <table> [WHERE <condition>];"
        )
        .expect("Error writing to str");
        writeln!(&mut out, "\tDisplay the selected columns from the table.")
            .expect("Error writing to str");
        writeln!(
            &mut out,
            "CREATE TABLE <table> (<column_name> <data_type> [PRIMARY_KEY|NOT_NULL|UNIQUE], ...);"
        )
        .expect("Error writing to str");
        writeln!(&mut out, "\tCreate a new table with the specified columns.")
            .expect("Error writing to str");
        writeln!(
            &mut out,
            "INSERT INTO <table> [(column1, ...)] VALUES (<value>, ...);"
        )
        .expect("Error writing to str");
        writeln!(&mut out, "\tInsert a new row into the table.").expect("Error writing to str");
        writeln!(
            &mut out,
            "UPDATE <table> SET <column> = <value> [WHERE <condition>];"
        )
        .expect("Error writing to str");
        writeln!(&mut out, "\tUpdate the specified column in the table.")
            .expect("Error writing to str");
        writeln!(&mut out, "DELETE FROM <table> [WHERE <condition>];")
            .expect("Error writing to str");
        writeln!(&mut out, "\tDelete rows from the table.").expect("Error writing to str");
        writeln!(&mut out, "DROP TABLE <table>;").expect("Error writing to str");
        writeln!(&mut out, "\tDelete the table from the database.").expect("Error writing to str");
        writeln!(&mut out, "HELP;").expect("Error writing to str");
        writeln!(&mut out, "\tDisplay this help message.").expect("Error writing to str");
        writeln!(&mut out, "EXIT;").expect("Error writing to str");
        writeln!(&mut out, "\tExit the database.").expect("Error writing to str");
        Ok(out)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

fn cols_vec(
    table_name: &str,
    col_name: &str,
    data_type: i8,
    pos: i8,
    nullable: i8,
    key: &str,
) -> Vec<DataType> {
    let ck = if key.is_empty() {
        DataType::Null
    } else {
        DataType::Text(key.to_string())
    };
    vec![
        DataType::Text(table_name.to_string()),
        DataType::Text(col_name.to_string()),
        DataType::TinyInt(data_type),
        DataType::TinyInt(pos),
        DataType::TinyInt(nullable),
        ck,
    ]
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use serial_test::serial;

    use super::*;

    fn setup_db() -> Database {
        teardown_db();
        Database::new()
    }

    fn teardown_db() {
        let system_dir = std::path::Path::new(SYSTEM_DIR);
        if system_dir.exists() {
            std::fs::remove_dir_all(SYSTEM_DIR).ok();
        }
        let user_dir = std::path::Path::new(USER_DIR);
        if user_dir.exists() {
            std::fs::remove_dir_all(USER_DIR).ok();
        }
    }

    fn setup_db_with_table() -> Database {
        let mut db = setup_db();
        db.parse_user_input("CREATE TABLE test (id INT, name TEXT);")
            .expect("Failed creating table");
        db
    }

    #[serial]
    #[test]
    fn test_initialize() {
        let mut db = setup_db();
        assert!(!db.table_table.is_empty());
        assert!(!db.column_table.is_empty());
        let tables = db.table_table.search(None, DataType::Null, "=").unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(
            tables[0].values[0],
            DataType::Text("meta_tables".to_string())
        );
        assert_eq!(
            tables[1].values[0],
            DataType::Text("meta_columns".to_string())
        );
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_create_table() {
        let mut db = setup_db();
        db.parse_user_input("CREATE TABLE test (id INT PRIMARY_KEY, name TEXT);")
            .expect("Failed creating table");
        let table = db.load_table("test").expect("Table not found");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.has_index(), Some("id"));
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_drop_table() {
        let mut db = setup_db();
        db.parse_user_input("CREATE TABLE test (id INT PRIMARY_KEY, name TEXT);")
            .expect("Failed creating table");
        let index_path = format!("{}/test.id.ndx", USER_DIR);
        let idx_file = std::path::Path::new(&index_path);
        assert!(db.load_table("test").is_some());
        assert!(idx_file.exists());
        db.parse_user_input("DROP TABLE test;")
            .expect("Failed dropping table");
        assert!(db.load_table("test").is_none());
        assert!(!idx_file.exists());
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_insert_command() {
        let mut db = setup_db_with_table();
        db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test');")
            .expect("Failed inserting 1");
        db.parse_user_input("INSERT INTO test VALUES (2, 'test2');")
            .expect("Failed inserting 2");
        db.parse_user_input("INSERT INTO test VALUES (3, 'test3');")
            .expect("Failed inserting 3");

        let records = db
            .load_table("test")
            .expect("Table not found")
            .search(None, DataType::Null, "=")
            .expect("Failed searching");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].values[0], DataType::Int(1));
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_nullable() {
        let mut db = setup_db();
        db.parse_user_input("CREATE TABLE test (id INT, name TEXT);")
            .expect("Failed creating table");
        db.parse_user_input("INSERT INTO test (id) VALUES (1);")
            .expect("Failed inserting 1");
        let mut table = db.load_table("test").expect("Table not found");
        assert_eq!(table.len(), 1);
        let records = table
            .search(None, DataType::Null, "=")
            .expect("Failed searching");
        let vals = &records[0].values;
        assert_eq!(vals[0], DataType::Int(1));
        assert_eq!(vals[1], DataType::Null);
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_unique() {
        let mut db = setup_db();
        db.parse_user_input("CREATE TABLE test (id INT UNIQUE, name TEXT);")
            .expect("Failed creating table");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test');")
            .expect("Failed inserting 1");
        let res = db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test2');");
        assert!(res.is_err());
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_delete_command() {
        let mut db = setup_db_with_table();
        db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test');")
            .expect("Failed inserting 1");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (2, 'test2');")
            .expect("Failed inserting 2");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (3, 'test3');")
            .expect("Failed inserting 3");
        db.parse_user_input("DELETE FROM test WHERE id = 2;")
            .expect("Failed deleting");
        let mut table = db.load_table("test").expect("Table not found");
        let records = table
            .search(None, DataType::Null, "=")
            .expect("Failed searching");
        assert_eq!(records[0].values[0], DataType::Int(1));
        assert_eq!(records[1].values[0], DataType::Int(3));
        assert_eq!(table.len(), 2);
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_update_command() {
        let mut db = setup_db_with_table();
        db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test');")
            .expect("Failed inserting 1");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (2, 'test2');")
            .expect("Failed inserting 2");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (3, 'test3');")
            .expect("Failed inserting 3");
        let mut table = db.load_table("test").expect("Table not found");
        assert_eq!(table.len(), 3);
        let records = table
            .search(Some("id"), DataType::Int(2), "=")
            .expect("Failed searching");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].values[1], DataType::Text("'test2'".to_string()));
        db.parse_user_input("UPDATE test SET name = 'test4' WHERE id = 2;")
            .expect("Failed updating");
        let records = table
            .search(Some("id"), DataType::Int(2), "=")
            .expect("Failed searching");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].values[1], DataType::Text("'test4'".to_string()));
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_select_command() {
        let mut db = setup_db_with_table();
        db.parse_user_input("INSERT INTO test (id, name) VALUES (1, 'test');")
            .expect("Failed inserting 1");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (2, 'test2');")
            .expect("Failed inserting 2");
        db.parse_user_input("INSERT INTO test (id, name) VALUES (3, 'test3');")
            .expect("Failed inserting 3");
        let res = db
            .parse_user_input("SELECT * FROM test;")
            .expect("Failed selecting");
        let res_str = "Table: test\nid name \n1 'test' \n2 'test2' \n3 'test3' \n";
        assert_eq!(res, res_str);
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_select_from_system() {
        let mut db = setup_db();
        let res = db.parse_user_input("SELECT * FROM meta_tables;");
        assert!(res.is_ok());
        assert_ne!(res.unwrap(), "No records found.");
        let res = db.parse_user_input("SELECT * FROM meta_columns;");
        assert!(res.is_ok());
        assert_ne!(res.unwrap(), "No records found.");
        teardown_db();
    }

    #[serial]
    #[test]
    fn test_date_types() {
        let mut db = setup_db();
        db.parse_user_input(
            "CREATE TABLE test (id INT, date DATE, time TIME, datetime DATETIME, year YEAR);",
        )
        .expect("Failed creating table");
        db.parse_user_input(
            "INSERT INTO test VALUES (1, 2021-01-01, 12:00:00, 2021-01-01 12:00:00, 2021);",
        )
        .expect("Failed inserting");
        let mut table = db.load_table("test").expect("Table not found");
        assert_eq!(table.len(), 1);
        let records = table
            .search(None, DataType::Null, "=")
            .expect("Failed searching");
        assert_eq!(records[0].values[0], DataType::Int(1));
        let year = 21;
        let date = NaiveDate::from_ymd_opt(2021, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let time = NaiveTime::from_hms_opt(12, 0, 0)
            .unwrap()
            .signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
            .num_seconds();
        let datetime = NaiveDate::from_ymd_opt(2021, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(records[0].values[1], DataType::Date(date));
        assert_eq!(records[0].values[2], DataType::Time(time as i32));
        assert_eq!(records[0].values[3], DataType::DateTime(datetime));
        assert_eq!(records[0].values[4], DataType::Year(year));
        let res = db
            .parse_user_input("SELECT * FROM test;")
            .expect("Failed selecting");
        let res_str = "Table: test\nid date time datetime year \n1 2021-01-01 12:00:00 2021-01-01 12:00:00 2021 \n";
        assert_eq!(res, res_str);
        teardown_db();
    }
}
