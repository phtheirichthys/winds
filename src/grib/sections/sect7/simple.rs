use num::ToPrimitive;
use crate::grib::GribError;
use crate::grib::GribError::ParseError;
use crate::grib::sections::sect5::{Data, DataRepresentationDefinition};
use crate::grib::sections::sect7::Grib2DataDecoder;
use crate::grib::utils::BitwiseIterator;

pub(crate) struct GridPointDataSimplePackingDecoder {}

impl Grib2DataDecoder for GridPointDataSimplePackingDecoder {
    fn decode(&self, data_repr_def: &DataRepresentationDefinition, slice: &Box<[u8]>) -> crate::grib::Result<Box<[f64]>> {

        let data = match &data_repr_def.data {
            Data::Data0(data) => data,
            _ => {
                return Err(ParseError(String::from("Wrong decoder")));
            }
        };

        if data.num_bits == 0 {
            let decoded = vec![data.reference_value as f64; data_repr_def.num_points as usize];
            return Ok(decoded.into_boxed_slice());
        }

        let decoder = SimpleDecoderIterator::new(BitwiseIterator::<u32>::new(slice, data.num_bits), data.reference_value as f64, data.binary_scale_factor, data.decimal_scale_factor);
        let decoded: Vec<f64> = decoder.collect();

        if decoded.len() != data_repr_def.num_points {
            return Err(GribError::DecodeError(String::from("Length Mismatch")));
        }

        Ok(decoded.into_boxed_slice())
    }
}

pub(crate) struct SimpleDecoderIterator<I: Iterator<Item = N>, N: ToPrimitive> {
    bitwise_iter: I,
    reference_value: f64,
    binary_scale: f64,
    decimal_scale: f64,
}

impl<I: Iterator<Item = N>, N: ToPrimitive> SimpleDecoderIterator<I, N> {
    pub(crate) fn new(bitwise_iter: I, reference_value: f64, binary_scale_factor: i16, decimal_scale_factor: i16) -> Self {
        Self {
            bitwise_iter,
            reference_value,
            binary_scale: 2_f64.powi(binary_scale_factor as i32),
            decimal_scale: 10_f64.powi(-decimal_scale_factor as i32),
        }
    }
}

impl<I: Iterator<Item = N>, N: ToPrimitive> Iterator for SimpleDecoderIterator<I, N> {
    type Item = f64;

    fn next(&mut self) -> Option<f64> {
        match self.bitwise_iter.next() {
            Some(encoded) => {
                let value = (self.reference_value + encoded.to_f64().unwrap() * self.binary_scale) * self.decimal_scale;
                Some(value)
            }
            _ => None,
        }
    }
}
