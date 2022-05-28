use crate::grib::sections::sect5::DataRepresentationDefinition;

pub(crate) mod simple;
pub(crate) mod complex;
mod groups;
pub(crate) mod complex_spacial_diff;

pub(crate) trait Grib2DataDecoder {
    fn decode(&self, data_repr_def: &DataRepresentationDefinition, slice: &Box<[u8]>) -> crate::grib::Result<Box<[f64]>>;
}