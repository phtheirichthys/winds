use crate::grib::utils::{Buffer, GribInt};
use crate::read_as;

pub struct DataRepresentationDefinition {
    pub num_points: usize,
    pub template_number: u16,
    pub data: Data
}

pub enum Data {
    Data0(Data0),
    Data2(Data2),
    Data3(Data3),
    Unknown(Vec<u8>)
}

impl Data {
    pub(crate) fn from_template(template_number: u16, bytes: Vec<u8>) -> crate::grib::Result<Self> {

        let mut buf = Buffer::new(bytes);

        match template_number {
            0 => {
                Ok(Data::Data0(Data0 {
                    reference_value: buf.read(),
                    binary_scale_factor: buf.read::<u16>().as_grib_int(),
                    decimal_scale_factor: buf.read::<u16>().as_grib_int(),
                    num_bits: buf.read::<u8>() as usize,
                    values_type: buf.read(),
                }))
            }
            2 => {
                Ok(Data::Data2(Data2 {
                    reference_value: buf.read(),
                    binary_scale_factor: buf.read::<u16>().as_grib_int(),
                    decimal_scale_factor: buf.read::<u16>().as_grib_int(),
                    num_bits: buf.read::<u8>() as usize,
                    values_type: buf.read(),
                    group_method: buf.read(),
                    missing_value: buf.read(),
                    missing_substitute_primary: buf.read(),
                    missing_substitute_secondary: buf.read(),
                    group_definition: GroupDefinition {
                        num_groups: buf.read::<u8>() as usize,
                        group_widths_reference: buf.read(),
                        group_widths_num_bits: buf.read::<u8>() as usize,
                        group_lengths_reference: buf.read(),
                        group_lengths_increment: buf.read(),
                        group_lengths_last: buf.read(),
                        group_scaled_lengths_num_bits: buf.read::<u8>() as usize,
                    },
                }))
            }
            3 => {
                Ok(Data::Data3(Data3 {
                    reference_value: buf.read(),
                    binary_scale_factor: buf.read::<u16>().as_grib_int(),
                    decimal_scale_factor: buf.read::<u16>().as_grib_int(),
                    num_bits: buf.read::<u8>() as usize,
                    values_type: buf.read(),
                    group_method: buf.read(),
                    missing_value: buf.read(),
                    missing_substitute_primary: buf.read(),
                    missing_substitute_secondary: buf.read(),
                    group_definition: GroupDefinition {
                        num_groups: buf.read::<u32>() as usize,
                        group_widths_reference: buf.read(),
                        group_widths_num_bits: buf.read::<u8>() as usize,
                        group_lengths_reference: buf.read(),
                        group_lengths_increment: buf.read(),
                        group_lengths_last: buf.read(),
                        group_scaled_lengths_num_bits: buf.read::<u8>() as usize,
                    },
                    spacial_difference_order: buf.read(),
                    spacial_difference_size: buf.read()
                }))
            }
            _ => {
                Ok(Data::Unknown(buf.bytes))
            }
        }
    }
}

pub struct Data0 {
    pub reference_value: f32,
    pub binary_scale_factor: i16,
    pub decimal_scale_factor: i16,
    pub num_bits: usize,
    pub values_type: u8,
}
pub struct GroupDefinition {
    pub num_groups: usize,
    pub group_widths_reference: u8,
    pub group_widths_num_bits: usize,
    pub group_lengths_reference: u32,
    pub group_lengths_increment: u8,
    pub group_lengths_last: u32,
    pub group_scaled_lengths_num_bits: usize
}

pub struct Data2 {
    pub reference_value: f32,
    pub binary_scale_factor: i16,
    pub decimal_scale_factor: i16,
    pub num_bits: usize,
    pub values_type: u8,
    pub group_method: u8,
    pub missing_value: u8,
    pub missing_substitute_primary: u32,
    pub missing_substitute_secondary: u32,
    pub group_definition: GroupDefinition,
}

pub struct Data3 {
    pub reference_value: f32,
    pub binary_scale_factor: i16,
    pub decimal_scale_factor: i16,
    pub num_bits: usize,
    pub values_type: u8,
    pub group_method: u8,
    pub missing_value: u8,
    pub missing_substitute_primary: u32,
    pub missing_substitute_secondary: u32,
    pub group_definition: GroupDefinition,
    pub spacial_difference_order: u8,
    pub spacial_difference_size: u8
}