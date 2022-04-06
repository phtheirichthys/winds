use crate::grib::GribError::ParseError;
use crate::grib::sections::sect5::{Data, DataRepresentationDefinition};
use crate::grib::sections::sect7::{Grib2DataDecoder, groups};
use crate::grib::utils::{BitwiseIterator, GribInt};
use std::iter;
use crate::grib::sections::sect7::simple::SimpleDecoderIterator;

pub(crate) struct GridPointDataComplexPackingDecoder {}

impl Grib2DataDecoder for GridPointDataComplexPackingDecoder {
    fn decode(&self, data_repr_def: &DataRepresentationDefinition, slice: &Box<[u8]>) -> crate::grib::Result<Box<[f64]>> {

        let data = match &data_repr_def.data {
            Data::Data2(data) => data,
            _ => {
                return Err(ParseError(String::from("Wrong decoder")));
            }
        };

        let (group_iter, groups_num_bytes) = groups::decode(data_repr_def, slice)?;

        Ok(
            SimpleDecoderIterator::new(
                ComplexPackingDecoderIterator::new(&slice[groups_num_bytes..], group_iter).flatten(),
                data.reference_value as f64, data.binary_scale_factor, data.decimal_scale_factor
            ).collect()
        )
    }
}

pub(crate) struct ComplexPackingDecoderIterator<'a, I: Iterator<Item = (i64, usize, usize)>> {
    slice: &'a [u8],
    groups_iter: I,
    pos: usize,
    start_offset_num_bits: usize,
}

impl<'a, I: Iterator<Item = (i64, usize, usize)>> ComplexPackingDecoderIterator<'a, I> {
    pub(crate) fn new(slice: &'a [u8], groups_iter: I) -> Self {
        Self {
            slice,
            groups_iter,
            pos: 0,
            start_offset_num_bits: 0,
        }
    }
}

impl<'a, I: Iterator<Item = (i64, usize, usize)>> Iterator for ComplexPackingDecoderIterator<'a, I> {
    type Item = Vec<i64>;

    fn next(&mut self) -> Option<Vec<i64>> {

        match (self.groups_iter.next()) {
            Some((reference_value, width, length)) => {

                //let reference_value = reference_value as i32;
                let width = width as usize;
                let length = length as usize;

                let total_num_bits = width * length + self.start_offset_num_bits;
                let (pos_end, offset_num_bits) = (self.pos + total_num_bits / 8, total_num_bits % 8);
                let offset_byte = if offset_num_bits > 0 { 1 } else { 0 };
                let group_values =
                    BitwiseIterator::<u64>::new(&self.slice[self.pos..pos_end + offset_byte], width)
                        .with_offset(self.start_offset_num_bits)
                        .take(length)
                        .map(|v| reference_value + v.as_grib_int())
                        .collect::<Vec<i64>>();
                self.pos = pos_end;
                self.start_offset_num_bits = offset_num_bits;
                Some(group_values)
            }
            _ => None
        }
    }
}
