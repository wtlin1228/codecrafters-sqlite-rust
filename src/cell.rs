use super::{reader_utils::ReadeInto, serial_value::SerialValue};
use anyhow::{Context, Ok, Result};
use std::io::prelude::*;
use std::io::Cursor;

#[derive(Debug)]
pub struct TableLeafCell {
    pub row_id: u64,
    pub columns: Vec<SerialValue>,
}

impl TableLeafCell {
    pub fn parse(cell: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(cell);
        let _payload_size = reader.read_varint().context("Read varint - payload size")?;
        let row_id = reader.read_varint().context("Read varint - rowid")?;

        Ok(Self {
            row_id,
            columns: read_payload(reader)?,
        })
    }
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub row_id: u64,
    pub page_number_of_left_child: usize,
}

impl TableInteriorCell {
    pub fn parse(cell: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(cell);
        let page_number_of_left_child = reader
            .read_u32(4)
            .context("Read page number of left child")?
            as usize;
        let row_id = reader.read_varint().context("Read varint - rowid")?;

        Ok(Self {
            row_id,
            page_number_of_left_child,
        })
    }
}

#[derive(Debug)]
pub struct IndexLeafCell {
    pub row_id: u64,
    pub columns: Vec<SerialValue>,
}

impl IndexLeafCell {
    pub fn parse(cell: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(cell);
        let _number_of_bytes_of_payload = reader
            .read_varint()
            .context("Read varint - Number of bytes of payload")?
            as usize;
        let mut payload = read_payload(reader)?;
        let row_id = payload
            .pop()
            .context("Read row id from the payload's last item")?
            .into_u64()
            .context("Convert varint into u64 for row id")?;

        Ok(Self {
            row_id,
            columns: payload,
        })
    }

    // Our index is created on single column only, so the first column is what we need.
    pub fn get_first_column_value(&self) -> String {
        format!("{}", self.columns[0])
    }
}

#[derive(Debug)]
pub struct IndexInteriorCell {
    pub page_number_of_left_child: usize,
    pub row_id: u64,
    pub columns: Vec<SerialValue>,
}

impl IndexInteriorCell {
    pub fn parse(cell: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(cell);
        let page_number_of_left_child = reader
            .read_u32(4)
            .context("Read page number of left child")?
            as usize;
        let _number_of_bytes_of_payload = reader
            .read_varint()
            .context("Read varint - Number of bytes of payload")?
            as usize;
        let mut payload = read_payload(reader)?;
        let row_id = payload
            .pop()
            .context("Read row id from the payload's last item")?
            .into_u64()
            .context("Convert varint into u64 for row id")?;

        Ok(Self {
            page_number_of_left_child,
            row_id,
            columns: payload,
        })
    }

    // Our index is created on single column only, so the first column is what we need.
    pub fn get_first_column_value(&self) -> String {
        format!("{}", self.columns[0])
    }
}

fn read_payload(mut reader: impl Read + Seek) -> Result<Vec<SerialValue>> {
    let header_start = reader.stream_position()?;
    let header_size = reader.read_varint().context("Read varint - header size")?;
    let mut serial_types = vec![];
    while reader.stream_position()? < header_start + header_size as u64 {
        let serial_type = reader.read_varint().context("Read varint - serial type")?;
        serial_types.push(serial_type);
    }

    let mut columns = vec![];
    for serial_type in serial_types {
        columns.push(reader.read_serial_value(serial_type)?);
    }

    Ok(columns)
}
