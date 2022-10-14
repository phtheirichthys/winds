use crate::grib::GribError::ParseError;
use crate::grib::sections::sect5::{Data, DataRepresentationDefinition};
use crate::grib::sections::sect7::{Grib2DataDecoder, groups};
use crate::grib::utils::GribInt;
use crate::grib::sections::sect7::complex::ComplexPackingDecoderIterator;
use crate::grib::sections::sect7::simple::SimpleDecoderIterator;
use crate::read_as;

pub(crate) struct GridPointDataComplexPackingSpacialDiffDecoder {}

impl Grib2DataDecoder for GridPointDataComplexPackingSpacialDiffDecoder {
    fn decode(&self, data_repr_def: &DataRepresentationDefinition, slice: &Box<[u8]>) -> crate::grib::Result<Box<[f64]>> {

        let data = match &data_repr_def.data {
            Data::Data3(data) => data,
            _ => {
                return Err(ParseError(String::from("Wrong decoder")));
            }
        };

        let cpt: usize;
        let z1 = read_as!(u16, slice, 0).as_grib_int();
        let (z2, z_min) = {
            if data.spacial_difference_order == 2 {
                cpt = 6;
                (read_as!(u16, slice, 2).as_grib_int(), read_as!(u16, slice, 4).as_grib_int())
            } else {
                cpt = 4;
                (0, read_as!(u16, slice, 2).as_grib_int())

            }
        };

        let (group_iter, groups_num_bytes) = groups::decode(data_repr_def, &slice[cpt..])?;
        let to_skip = groups_num_bytes + cpt;

        //let spdiff_packed_iter = iter::once(z1).chain(iter::once(z2)).chain(ComplexPackingDecoderIterator::new(&slice[to_skip..], group_iter).flatten());
        let spdiff_packed_iter = ComplexPackingDecoderIterator::new(&slice[to_skip..], group_iter).flatten();

        let spdiff_unpacked = SpatialDiff2ndOrderDecodeIterator::new(spdiff_packed_iter);

        Ok(
            SimpleDecoderIterator::new(
                spdiff_unpacked,
                data.reference_value as f64, data.binary_scale_factor, data.decimal_scale_factor
            ).collect()
        )
    }
}

struct SpatialDiff2ndOrderDecodeIterator<I> {
    iter: I,
    count: usize,
    prev1: i64,
    prev2: i64,
}

impl<I> SpatialDiff2ndOrderDecodeIterator<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            count: 0,
            prev1: 0,
            prev2: 0,
        }
    }
}

impl<I: Iterator<Item = i64>> Iterator for SpatialDiff2ndOrderDecodeIterator<I> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        let count = self.count;
        self.count += 1;

        match (count, self.iter.next()) {
            (_, None) => None,
            (0, Some(v)) => {
                self.prev2 = v;
                Some(v)
            }
            (1, Some(v)) => {
                self.prev1 = v;
                Some(v)
            }
            (_, Some(v)) => {
                let v = v + 2 * self.prev1 - self.prev2;

                (self.prev2, self.prev1) = (self.prev1, v);
                Some(v)
            },
        }

    }
}
