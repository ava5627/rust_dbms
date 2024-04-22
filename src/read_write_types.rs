use std::io::{Read, Write};
use crate::constants::DataType;

pub trait ReadWriteTypes: Read + Write {

    fn write_i8(&mut self, value: i8) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_u8(&mut self, value: u8) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_i16(&mut self, value: i16) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_u16(&mut self, value: u16) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_i32(&mut self, value: i32) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_u32(&mut self, value: u32) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_i64(&mut self, value: i64) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_f32(&mut self, value: f32) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_f64(&mut self, value: f64) -> usize {
        self.write(&value.to_le_bytes()).expect("Failed writing")
    }

    fn write_string(&mut self, value: &str) -> usize {
        self.write(value.as_bytes()).expect("Failed writing")
    }

    fn read_i8(&mut self) -> i8 {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).expect("Failed Reading");
        i8::from_le_bytes(buf)
    }

    fn read_u8(&mut self) -> u8 {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).expect("Failed Reading");
        u8::from_le_bytes(buf)
    }

    fn read_i16(&mut self) -> i16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).expect("Failed Reading");
        i16::from_le_bytes(buf)
    }

    fn read_u16(&mut self) -> u16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).expect("Failed Reading");
        u16::from_le_bytes(buf)
    }

    fn read_i32(&mut self) -> i32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).expect("Failed Reading");
        i32::from_le_bytes(buf)
    }

    fn read_u32(&mut self) -> u32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).expect("Failed Reading");
        u32::from_le_bytes(buf)
    }

    fn read_i64(&mut self) -> i64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).expect("Failed Reading");
        i64::from_le_bytes(buf)
    }

    fn read_f32(&mut self) -> f32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).expect("Failed Reading");
        f32::from_le_bytes(buf)
    }

    fn read_f64(&mut self) -> f64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).expect("Failed Reading");
        f64::from_le_bytes(buf)
    }

    fn read_string(&mut self, length: usize) -> String {
        let mut buf = vec![0; length];
        self.read_exact(&mut buf).expect("Failed Reading");
        String::from_utf8(buf).unwrap()
    }

    fn write_value(&mut self, value: DataType) -> usize {
        let buf: Vec<u8> = (&value).into();
        self.write(&buf).expect("Failed writing")
    }

    fn read_value(&mut self, data_type: u8) -> DataType {
        let size = DataType::size_type(data_type);
        let mut buf = vec![0; size as usize];
        self.read_exact(&mut buf).expect("Failed Reading");
        
        match (data_type, buf).try_into() {
            Ok(value) => value,
            Err(e) => panic!("Failed to convert value: {}", e),
        }
    }
}
