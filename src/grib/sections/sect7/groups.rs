use std::iter;
use num::ToPrimitive;
use crate::grib::GribError::ParseError;
use crate::grib::sections::sect5::{Data, Data2, Data3, DataRepresentationDefinition};
use crate::grib::utils::BitwiseIterator;

pub(crate) fn decode<'a>(data_repr_def: &'a DataRepresentationDefinition, slice: &'a [u8]) -> crate::grib::Result<(impl Iterator<Item = (i64, usize, usize)> + 'a, usize)> {
    let (num_bits, group_definition) = match &data_repr_def.data {
        Data::Data2(Data2 { num_bits, group_definition, .. }) => (num_bits, group_definition),
        Data::Data3(Data3 { num_bits, group_definition, .. }) => (num_bits, group_definition),
        _ => {
            return Err(ParseError(String::from("Wrong decoder")));
        }
    };

    fn octet_length(num_bits: &usize, num_groups: usize) -> usize {
        let total_bit = num_groups * num_bits;
        let total_octet: f32 = total_bit as f32 / 8f32;
        total_octet.ceil() as usize
    }

    let (group_references_start, group_references_end) = (0, octet_length(num_bits, group_definition.num_groups));
    let references_iter = BitwiseIterator::<u64>::new(&slice[group_references_start..group_references_end], *num_bits)
        .take(group_definition.num_groups as usize);

    let (group_widths_start, group_widths_end) = (group_references_end, group_references_end + octet_length(&group_definition.group_widths_num_bits, group_definition.num_groups));
    let widths_iter = BitwiseIterator::<u64>::new(&slice[group_widths_start..group_widths_end], group_definition.group_widths_num_bits)
        .map(|v| u64::from(group_definition.group_widths_reference) + v)
        .take(group_definition.num_groups as usize);

    let (group_lengths_start, group_lengths_end) = (group_widths_end, group_widths_end + octet_length(&group_definition.group_scaled_lengths_num_bits, group_definition.num_groups));
    let lengths_iter = BitwiseIterator::<u64>::new(&slice[group_lengths_start..group_lengths_end], group_definition.group_scaled_lengths_num_bits)
        .take(group_definition.num_groups - 1)
        .map(|v| u64::from(group_definition.group_lengths_reference) + u64::from(group_definition.group_lengths_increment) * v)
        .chain(iter::once(u64::from(group_definition.group_lengths_last)));

    let groups = GroupsIterator::new(references_iter, widths_iter, lengths_iter);

    Ok((groups, group_lengths_end))
}

pub(crate) struct GroupsIterator<I: Iterator<Item = u64>, J: Iterator<Item = u64>, K: Iterator<Item = u64>>
{
    references_iter: I,
    widths_iter: J,
    lengths_iter: K,
}

impl<I: Iterator<Item = u64>, J: Iterator<Item = u64>, K: Iterator<Item = u64>> GroupsIterator<I, J, K>
{
    fn new(references_iter: I, widths_iter: J, lengths_iter: K) -> Self {
        Self {
            references_iter,
            widths_iter,
            lengths_iter,
        }
    }
}

impl<I: Iterator<Item = u64>, J: Iterator<Item = u64>, K: Iterator<Item = u64>> Iterator for GroupsIterator<I, J, K> {
    type Item = (i64, usize, usize);

    fn next(&mut self) -> Option<(i64, usize, usize)> {

        match (self.references_iter.next(), self.widths_iter.next(), self.lengths_iter.next()) {
            (Some(reference_value), Some(width), Some(length)) => {
                Some((reference_value as i64, width as usize, length as usize))
            }
            _ => None
        }
    }
}

