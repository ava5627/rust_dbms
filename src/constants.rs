use std::fmt::Display;
use std::str::FromStr;
use DataType::*;
extern crate chrono;

use chrono::{DateTime as ChronoDateTime, NaiveDate, NaiveDateTime};
use chrono::{NaiveTime, Utc};

pub const PAGE_SIZE: u64 = 512;
#[cfg(not(test))]
pub const SYSTEM_DIR: &str = "data/system";
#[cfg(test)]
pub const SYSTEM_DIR: &str = "data/test/system";
#[cfg(not(test))]
pub const USER_DIR: &str = "data/user";
#[cfg(test)]
pub const USER_DIR: &str = "data/test/user";
pub const TABLE_TABLE: &str = "meta_tables";
pub const COLUMN_TABLE: &str = "meta_columns";

pub const PROMPT: &str = "db > ";

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
            DataType::Text(s) => s.len() as u16,
            DataType::Unused => unreachable!("Unused data type has no size"),
        }
    }

    pub fn size_type(id: u8) -> u8 {
        match id {
            0x00 => 0,
            0x01 | 0x08 => 1,
            0x02 => 2,
            0x03 | 0x09 | 0x05 => 4,
            0x04 | 0x0A | 0x0B | 0x06 => 8,
            0x07 => unreachable!("Unused data type has no size"),
            v => v - 0x0C,
        }
    }

    pub fn parse_str(data_type: DataType, value: &str) -> Result<DataType, String> {
        let value = value.trim();
        let pared = match data_type {
            DataType::TinyInt(_) => DataType::TinyInt(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::SmallInt(_) => DataType::SmallInt(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::Int(_) => DataType::Int(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::BigInt(_) => DataType::BigInt(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::Float(_) => DataType::Float(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::Double(_) => DataType::Double(
                value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {}", value, data_type))?,
            ),
            DataType::Year(_) => {
                let year: i32 = value
                    .parse()
                    .map_err(|_| format!("{} cannot be parsed into {:?}", value, data_type))?;
                DataType::Year((year - 2000) as i8)
            }
            DataType::Time(_) => {
                let time = NaiveTime::parse_from_str(value, "%H:%M:%S")
                    .map_err(|_| format!("{} cannot be parsed into {:?}", value, data_type))?;
                let seconds = time
                    .signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                    .num_seconds();
                DataType::Time(seconds as i32)
            }
            DataType::DateTime(_) => {
                let second = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                    .map_err(|_| format!("{} cannot be parsed into {:?}", value, data_type))?;
                DataType::DateTime(second.and_utc().timestamp())
            }
            DataType::Date(_) => {
                let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .map_err(|_| format!("{} cannot be parsed into {:?}", value, data_type))?;
                DataType::Date(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
            }
            DataType::Text(_) => DataType::Text(value.to_string()),
            _ => unreachable!("Invalid data type"),
        };
        Ok(pared)
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
        match self {
            Null => write!(f, "NULL"),
            TinyInt(v) => write!(f, "{}", v),
            SmallInt(v) => write!(f, "{}", v),
            Int(v) => write!(f, "{}", v),
            BigInt(v) => write!(f, "{}", v),
            Float(v) => write!(f, "{}", v),
            Double(v) => write!(f, "{}", v),
            DateTime(v) => write!(
                f,
                "{}",
                ChronoDateTime::<Utc>::from_timestamp(*v, 0)
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
            ),
            Time(v) => write!(
                f,
                "{}",
                ChronoDateTime::<Utc>::from_timestamp(*v as i64, 0)
                    .unwrap()
                    .format("%H:%M:%S")
            ),
            Date(v) => write!(
                f,
                "{}",
                ChronoDateTime::<Utc>::from_timestamp(*v, 0)
                    .unwrap()
                    .format("%Y-%m-%d")
            ),
            Year(v) => write!(
                f,
                "{}",
                NaiveDate::from_ymd_opt(2000 + *v as i32, 1, 1)
                    .unwrap()
                    .format("%Y")
            ),
            Text(v) => write!(f, "{}", v),
            Unused => write!(f, "UNUSED"),
        }
    }
}

impl From<u8> for DataType {
    fn from(value: u8) -> Self {
        match value {
            0x00 => Null,
            0x01 => TinyInt(0),
            0x02 => SmallInt(0),
            0x03 => Int(0),
            0x04 => BigInt(0),
            0x05 => Float(0.0),
            0x06 => Double(0.0),
            0x07 => Unused,
            0x08 => Year(0),
            0x09 => Time(0),
            0x0A => DateTime(0),
            0x0B => Date(0),
            _ => Text("".to_string()),
        }
    }
}

impl TryFrom<(u8, Vec<u8>)> for DataType {
    type Error = anyhow::Error;

    fn try_from(value: (u8, Vec<u8>)) -> anyhow::Result<Self> {
        let data_type = DataType::from(value.0);
        let slice = value.1.as_slice();
        Ok(match data_type {
            Null => Null,
            TinyInt(_) => TinyInt(i8::from_le_bytes(slice.try_into()?)),
            SmallInt(_) => SmallInt(i16::from_le_bytes(slice.try_into()?)),
            Int(_) => Int(i32::from_le_bytes(slice.try_into()?)),
            BigInt(_) => BigInt(i64::from_le_bytes(slice.try_into()?)),
            Float(_) => Float(f32::from_le_bytes(slice.try_into()?)),
            Double(_) => Double(f64::from_le_bytes(slice.try_into()?)),
            Unused => Unused,
            Year(_) => Year(i8::from_le_bytes(slice.try_into()?)),
            Time(_) => Time(i32::from_le_bytes(slice.try_into()?)),
            DateTime(_) => DateTime(i64::from_le_bytes(slice.try_into()?)),
            Date(_) => Date(i64::from_le_bytes(slice.try_into()?)),
            Text(_) => Text(String::from_utf8(slice.to_vec())?),
        })
    }
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "tinyint" => Ok(DataType::TinyInt(0)),
            "smallint" => Ok(DataType::SmallInt(0)),
            "int" => Ok(DataType::Int(0)),
            "bigint" => Ok(DataType::BigInt(0)),
            "float" => Ok(DataType::Float(0.0)),
            "double" => Ok(DataType::Double(0.0)),
            "year" => Ok(DataType::Year(0)),
            "time" => Ok(DataType::Time(0)),
            "datetime" => Ok(DataType::DateTime(0)),
            "date" => Ok(DataType::Date(0)),
            "text" => Ok(DataType::Text("".to_string())),
            _ => Err("Invalid DataType".to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_parse_str() {
        use super::*;
        let year = DataType::Year(10);
        let time = DataType::Time(3600);
        let date = DataType::Date(1614556800);
        let datetime = DataType::DateTime(1614560400);
        let year_str = "2010";
        let time_str = "01:00:00";
        let date_str = "2021-03-01";
        let datetime_str = "2021-03-01 01:00:00";
        assert_eq!(DataType::parse_str(Year(0), year_str).unwrap(), year);
        assert_eq!(DataType::parse_str(Time(0), time_str).unwrap(), time);
        assert_eq!(DataType::parse_str(Date(0), date_str).unwrap(), date);
        assert_eq!(
            DataType::parse_str(DateTime(0), datetime_str).unwrap(),
            datetime
        );
        assert_eq!("2010", format!("{}", year));
        assert_eq!("01:00:00", format!("{}", time));
        assert_eq!("2021-03-01", format!("{}", date));
        assert_eq!("2021-03-01 01:00:00", format!("{}", datetime));
    }
}
