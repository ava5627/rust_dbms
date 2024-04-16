use std::io::{Read, Write, Result};
use crate::constants::DataType;

pub trait ReadWriteTypes: Read + Write {

    fn write_i8(&mut self, value: i8) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_u8(&mut self, value: u8) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_i16(&mut self, value: i16) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_u16(&mut self, value: u16) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_i32(&mut self, value: i32) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_u32(&mut self, value: u32) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_i64(&mut self, value: i64) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_f32(&mut self, value: f32) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_f64(&mut self, value: f64) -> Result<usize> {
        self.write(&value.to_le_bytes())
    }

    fn write_string(&mut self, value: &str) -> Result<usize> {
        self.write(value.as_bytes())
    }

    fn read_i8(&mut self) -> Result<i8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(i8::from_le_bytes(buf))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(u8::from_le_bytes(buf))
    }

    fn read_i16(&mut self) -> Result<i16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    fn read_u16(&mut self) -> Result<u16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    fn read_string(&mut self, length: usize) -> Result<String> {
        let mut buf = vec![0; length];
        self.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf).unwrap())
    }

    fn write_value(&mut self, value: DataType) -> Result<usize> {
        let buf: Vec<u8> = (&value).into();
        self.write(&buf)
    }

    fn read_value(&mut self, data_type: u8) -> Result<DataType> {
        let size = DataType::size_type(data_type);
        let mut buf = vec![0; size as usize];
        self.read_exact(&mut buf)?;
        let value = match (data_type, buf).try_into() {
            Ok(value) => value,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        };
        Ok(value)
    }
}
