use std::fmt::Display;
use DataType::*;
pub const PAGE_SIZE: u64 = 512;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PageType {
    IndexInterior = 0x02,
    TableInterior = 0x05,
    TableLeaf = 0x0A,
    IndexLeaf = 0x0D,
    Empty = 0x00,
    Invalid = 0xFF,
}

impl From<u8> for PageType {
    fn from(byte: u8) -> Self {
        match byte {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0A => PageType::TableLeaf,
            0x0D => PageType::IndexLeaf,
            _ => PageType::Invalid,
        }
    }
}

impl From<PageType> for u8 {
    fn from(page_type: PageType) -> Self {
        match page_type {
            PageType::IndexInterior => 0x02,
            PageType::TableInterior => 0x05,
            PageType::TableLeaf => 0x0A,
            PageType::IndexLeaf => 0x0D,
            _ => 0xFF,
        }
    }
}

pub enum FileType {
    Table,
    Index,
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum DataType {
    Null,
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Unused,
    Year(i8),
    Time(i32),
    DateTime(i64),
    Date(i64),
    Text(String),
}

impl DataType {
    pub fn size(&self) -> u16 {
        match self {
            DataType::Null => 0,
            DataType::TinyInt(_) | DataType::Year(_) => 1,
            DataType::SmallInt(_) => 2,
            DataType::Int(_) | DataType::Time(_) | DataType::Float(_) => 4,
            DataType::BigInt(_)
            | DataType::DateTime(_)
            | DataType::Date(_)
            | DataType::Double(_) => 8,
            DataType::Text(s) => s.as_bytes().len() as u16,
            DataType::Unused => unreachable!("Unused data type has no size"),
        }
    }

    pub fn parse_str(data_type: DataType, value: &str) -> DataType {
        let value = value.trim();
        match data_type {
            DataType::TinyInt(_) => DataType::TinyInt(value.parse().unwrap()),
            DataType::SmallInt(_) => DataType::SmallInt(value.parse().unwrap()),
            DataType::Int(_) => DataType::Int(value.parse().unwrap()),
            DataType::BigInt(_) => DataType::BigInt(value.parse().unwrap()),
            DataType::Float(_) => DataType::Float(value.parse().unwrap()),
            DataType::Double(_) => DataType::Double(value.parse().unwrap()),
            DataType::Year(_) => DataType::Year(value.parse().unwrap()),
            DataType::Time(_) => DataType::Time(value.parse().unwrap()),
            DataType::DateTime(_) => DataType::DateTime(value.parse().unwrap()),
            DataType::Date(_) => DataType::Date(value.parse().unwrap()),
            DataType::Text(_) => DataType::Text(value.to_string()),
            _ => unreachable!("Invalid data type"),
        }
    }
}

impl From<&DataType> for u8 {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => 0x00,
            DataType::TinyInt(_) => 0x01,
            DataType::SmallInt(_) => 0x02,
            DataType::Int(_) => 0x03,
            DataType::BigInt(_) => 0x04,
            DataType::Float(_) => 0x05,
            DataType::Double(_) => 0x06,
            DataType::Unused => 0x07,
            DataType::Year(_) => 0x08,
            DataType::Time(_) => 0x09,
            DataType::DateTime(_) => 0x0A,
            DataType::Date(_) => 0x0B,
            DataType::Text(value) => 0x0C + value.len() as u8,
        }
    }
}

impl From<&DataType> for Vec<u8> {
    fn from(value: &DataType) -> Self {
        match value {
            Null => vec![],
            TinyInt(v) | Year(v) => v.to_le_bytes().to_vec(),
            SmallInt(v) => v.to_le_bytes().to_vec(),
            Int(v) | Time(v) => v.to_le_bytes().to_vec(),
            BigInt(v) | DateTime(v) | Date(v) => v.to_le_bytes().to_vec(),
            Float(v) => v.to_le_bytes().to_vec(),
            Double(v) => v.to_le_bytes().to_vec(),
            Text(v) => v.as_bytes().to_vec(),
            Unused => unreachable!("Unused data type should not be written to file"),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
