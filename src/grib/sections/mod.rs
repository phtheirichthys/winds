use crate::grib::sections::sect1::Identification;
use crate::grib::sections::sect3::GridDefinition;
use crate::grib::sections::sect4::ProductDefinition;
use crate::grib::sections::sect5::DataRepresentationDefinition;
use crate::grib::sections::sect6::BitMap;

pub mod sect1;
pub mod sect3;
pub mod sect4;
pub mod sect5;
pub mod sect6;
pub mod sect7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indicator {
    /// Discipline - GRIB Master Table Number (see Code Table 0.0)
    pub discipline: u8,
    /// Total length of GRIB message in octets (including Section 0)
    pub total_length: u64,
}

pub struct SectionHeader {
    /// Length : Length of the section in octets
    pub size: usize, // u32
    /// Number : Number of the section
    pub number: u8,
}

pub enum Section {
    Section0(Indicator),
    Section1(Identification),
    Section2,
    Section3(GridDefinition),
    Section4(ProductDefinition),
    Section5(DataRepresentationDefinition),
    Section6(BitMap),
    Section7(Box<[u8]>),
    Section8
}