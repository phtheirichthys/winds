pub mod sections;
mod utils;

use std::collections::HashMap;
use std::io::Read;
use chrono::{TimeZone, Utc};
use crate::grib::sections::{SectionHeader, Indicator, Section};
use crate::grib::sections::sect1::Identification;
use crate::grib::sections::sect3::{Grid, GridDefinition};
use crate::grib::sections::sect4::{Product, ProductDefinition};
use crate::grib::sections::sect5::{Data, DataRepresentationDefinition};
use crate::grib::sections::sect6::BitMap;
use crate::grib::sections::sect7::complex::GridPointDataComplexPackingDecoder;
use crate::grib::sections::sect7::complex_spacial_diff::GridPointDataComplexPackingSpacialDiffDecoder;
use crate::grib::sections::sect7::Grib2DataDecoder;
use crate::grib::sections::sect7::simple::GridPointDataSimplePackingDecoder;

const SECT0_IS_MAGIC: &[u8] = b"GRIB";
const SECT0_IS_MAGIC_SIZE: usize = SECT0_IS_MAGIC.len();
const SECT0_IS_SIZE: usize = 16;
const SECT_HEADER_SIZE: usize = 5;
const SECT8_ES_MAGIC: &[u8] = b"7777";
const SECT8_ES_SIZE: usize = SECT8_ES_MAGIC.len();

#[macro_export]
macro_rules! read_as {
    ($ty:ty, $buf:ident, $start:expr) => {{
        let end = $start + std::mem::size_of::<$ty>();
        <$ty>::from_be_bytes($buf[$start..end].try_into().unwrap())
    }};
}

#[macro_export]
macro_rules! skip {
    ($reader:ident, $len_extra:expr) => {{
        if $len_extra > 0 {
            let mut buf = vec![0; $len_extra];
            $reader.read_exact(&mut buf[..])?;
        }
    }};
}

pub(crate) struct Grib {
    pub(crate) messages: Vec<Message>,
}

pub(crate) struct Message {
    pub(crate) indicator: Indicator,
    pub(crate) identification: Identification,
    pub(crate) grid_definition: GridDefinition,
    pub(crate) product_definition: ProductDefinition,
    pub(crate) data_representation_definition: DataRepresentationDefinition,
    pub(crate) bitmap: BitMap,
    pub(crate) data: Box<[u8]>,
}

impl Message {
    pub(crate) fn decode(&self) -> Result<Box<[f64]>> {
        match &self.data_representation_definition.data {
            Data::Data0(data0) => {
                Ok(GridPointDataSimplePackingDecoder{}.decode(&self.data_representation_definition, &self.data)?)
            }
            Data::Data2(data2) => {
                Ok(GridPointDataComplexPackingDecoder{}.decode(&self.data_representation_definition, &self.data)?)
            }
            Data::Data3(data3) => {
                Ok(GridPointDataComplexPackingSpacialDiffDecoder{}.decode(&self.data_representation_definition, &self.data)?)
            }
            Data::Unknown(_) => {
                error!("Not implemented data decoder {}", self.data_representation_definition.template_number);
                Err(GribError::DecodeError(format!("Not implemented data decoder : {}", self.data_representation_definition.template_number)))
            }
        }
    }
}

pub(crate) fn from_reader<R: Read>(reader: R) -> Result<Grib, GribError> {

    let mut reader = GribReader::new(reader);

    let mut messages = Vec::new();

    while let Ok((header, sect0)) = reader.read_sect0() {
        let mut remaining_length = sect0.total_length - header.size as u64;
        let total_length = sect0.total_length.clone();

        let mut sections = vec![Section::Section0(sect0)];

        while remaining_length > 0 {

            debug!("Remaining size to read : {}/{}", remaining_length,  total_length);

            if remaining_length == SECT8_ES_SIZE as u64 {
                let section = reader.read_sect8_body(SECT8_ES_SIZE)?;

                sections.push(section);
                break;
            }

            let (header, section) = reader.read_section()?;

            remaining_length -= header.size as u64;
            sections.push(section);
        }

        let mut indicator = None;
        let mut identification = None;
        let mut grid_definition = None;
        let mut product_definition = None;
        let mut data_representation_definition = None;
        let mut bitmap = None;
        let mut data = None;

        for section in sections {
            match section {
                Section::Section0(section) => {
                    indicator = Some(section)
                }
                Section::Section1(section) => { identification = Some(section) }
                Section::Section2 => {}
                Section::Section3(section) => {grid_definition = Some(section)}
                Section::Section4(section) => {product_definition = Some(section)}
                Section::Section5(section) => {data_representation_definition = Some(section)}
                Section::Section6(section) => {bitmap = Some(section)}
                Section::Section7(section) => {data = Some(section)}
                Section::Section8 => {}
            }
        }

        messages.push(Message {
            indicator: indicator.ok_or(GribError::DecodeError(String::from("Missing Section 0")))?,
            identification: identification.ok_or(GribError::DecodeError(String::from("Missing Section 1")))?,
            grid_definition: grid_definition.ok_or(GribError::DecodeError(String::from("Missing Section 3")))?,
            product_definition: product_definition.ok_or(GribError::DecodeError(String::from("Missing Section 4")))?,
            data_representation_definition: data_representation_definition.ok_or(GribError::DecodeError(String::from("Missing Section 5")))?,
            bitmap: bitmap.ok_or(GribError::DecodeError(String::from("Missing Section 6")))?,
            data: data.ok_or(GribError::DecodeError(String::from("Missing Section 7")))?,
        })
    }

    Ok(Grib {
        messages
    })
}

struct GribReader<R: Read> {
    reader: R,
}

impl<R: Read> Read for GribReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.reader.read_exact(buf)
    }
}


impl<R: Read> GribReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }

    fn read_sect0(&mut self) -> Result<(SectionHeader, Indicator)> {

        let mut buf = [0; SECT0_IS_SIZE];

        self.read_exact(&mut buf[..])?;

        if &buf[0..SECT0_IS_MAGIC_SIZE] != SECT0_IS_MAGIC {
            return Err(GribError::NotGRIB());
        }

        let discipline = buf[6];
        let version = buf[7];

        if version != 2 {
            return Err(GribError::GRIBVersionMismatch(version));
        }

        let total_length = read_as!(u64, buf, 8);

        debug!("Read section {} : {}", 0, SECT0_IS_SIZE);

        Ok((SectionHeader {
            size: SECT0_IS_SIZE,
            number: 0
        }, Indicator
        {
            discipline,
            total_length,
        }))
    }

    fn read_header(&mut self) -> Result<SectionHeader> {
        let mut buf = [0; SECT_HEADER_SIZE];
        self.read_exact(&mut buf[..])?;

        let length = read_as!(u32, buf, 0);
        let number = buf[4];

        Ok(SectionHeader {
            size: length as usize,
            number,
        })
    }

    fn read_section(&mut self) -> Result<(SectionHeader, Section)> {

        let header = self.read_header()?;

        let body_size = header.size - SECT_HEADER_SIZE;
        debug!("Read section {} : {}(-{} : {})", header.number, header.size, SECT_HEADER_SIZE, body_size);
        let body = match header.number {
            1 => self.read_sect1_body(body_size)?,
            2 => self.read_sect2_body(body_size)?,
            3 => self.read_sect3_body(body_size)?,
            4 => self.read_sect4_body(body_size)?,
            5 => self.read_sect5_body(body_size)?,
            6 => self.read_sect6_body(body_size)?,
            7 => self.read_sect7_body(body_size)?,
            n => { return Err(GribError::UnknownSection(n)); },
        };

        Ok((header, body))
    }

    fn read_sect1_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = [0; 16]; // octet 6-21
        self.read_exact(&mut buf[..])?;

        skip!(self, body_size - buf.len());

        Ok(Section::Section1(Identification {
            centre_id: read_as!(u16, buf, 0),
            subcentre_id: read_as!(u16, buf, 2),
            master_table_version: buf[4],
            local_table_version: buf[5],
            ref_time_significance: buf[6],
            ref_time: Utc
                .ymd(read_as!(u16, buf, 7).into(), buf[9].into(), buf[10].into())
                .and_hms(buf[11].into(), buf[12].into(), buf[13].into()),
            prod_status: buf[14],
            data_type: buf[15],
        }))
    }

    fn read_sect2_body(&mut self, body_size: usize) -> Result<Section> {
        skip!(self, body_size);

        Ok(Section::Section2)
    }

    fn read_sect3_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = [0; 9]; // octet 6-14
        self.read_exact(&mut buf[..])?;

        let template_number = read_as!(u16, buf, 7);
        let optional_num_list_size = buf[5] as usize;

        let grid = Grid::from_template(template_number, {
            let template_size = body_size - buf.len() - optional_num_list_size;
            let mut buf = vec![0; template_size];
            self.read_exact(&mut buf[..])?;
            buf
        })?;

        Ok(Section::Section3(GridDefinition {
            source: buf[0],
            num_points: read_as!(u32, buf, 1) as usize,
            optional_num_list_size,
            optional_num_list_interpretation: buf[6],
            template_number,
            grid,
        }))
    }

    fn read_sect4_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = [0; 4]; // octet 6-9
        self.read_exact(&mut buf[..])?;

        let template_number = read_as!(u16, buf, 2);
        let num_coordinates = read_as!(u16, buf, 0);

        let product = Product::from_template(template_number, {
            let template_size = body_size - buf.len() - 4 * num_coordinates as usize;
            let mut buf = vec![0; template_size];
            self.read_exact(&mut buf[..])?;
            buf
        })?;

        Ok(Section::Section4(ProductDefinition {
            num_coordinates,
            template_number,
            product,
            coordinates: None
        }))
    }

    fn read_sect5_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = [0; 6]; // octet 6-11
        self.read_exact(&mut buf[..])?;

        let template_number = read_as!(u16, buf, 4);

        let data = Data::from_template(template_number, {
            let template_size = body_size - buf.len();
            let mut buf = vec![0; template_size];
            self.read_exact(&mut buf[..])?;
            buf
        })?;
        Ok(Section::Section5(DataRepresentationDefinition {
            num_points: read_as!(u32, buf, 0) as usize,
            template_number,
            data
        }))
    }

    fn read_sect6_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = [0; 1]; // octet 6
        self.read_exact(&mut buf[..])?;

        let bitmap_size = body_size - buf.len();
        let mut bitmap = vec![0; bitmap_size];
        self.read_exact(&mut bitmap[..])?;

        debug!("bitmap_size : {} - {} = {}", body_size, buf.len(), bitmap_size);

        Ok(Section::Section6(BitMap {
            bitmap_indicator: buf[0],
            bitmap
        }))
    }

    fn read_sect7_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = vec![0; body_size as usize];
        self.read_exact(&mut buf[..])?;

        Ok(Section::Section7(buf.into_boxed_slice()))
    }

    fn read_sect8_body(&mut self, body_size: usize) -> Result<Section> {
        let mut buf = vec![0; body_size as usize];
        self.read_exact(&mut buf[..])?;

        if buf[..] != SECT8_ES_MAGIC[..] {
            return Err(GribError::EndSectionMismatch());
        }

        Ok(Section::Section8)
    }
}
pub type Result<T, E = GribError> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum GribError {
    #[error("GribError")]
    GribError(),

    #[error("StdError({0})")]
    StdError(#[from] std::io::Error),

    #[error("GribError")]
    NotGRIB(),

    #[error("GRIBVersionMismatch({0})")]
    GRIBVersionMismatch(u8),

    #[error("EndSectionMismatch")]
    EndSectionMismatch(),

    #[error("UnknownSection({0})")]
    UnknownSection(u8),

    #[error("ParseError({0})")]
    ParseError(String),

    #[error("DecodeError({0})")]
    DecodeError(String)
}
