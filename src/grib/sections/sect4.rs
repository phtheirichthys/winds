use chrono::Duration;
use crate::grib::GribError;
use crate::read_as;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProductDefinition {
    /// Number of coordinate values after Template
    pub num_coordinates: u16,
    /// Product Definition Template Number
    pub template_number: u16,
    pub product: Product,
    pub coordinates: Option<Box<[u8]>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Product {
    Product0(Product0),
    Unknown(Vec<u8>)
}

impl Product {
    pub(crate) fn from_template(template_number: u16, buf: Vec<u8>) -> crate::grib::Result<Self> {

        match template_number {
            0 => {
                Ok(Product::Product0(Product0 {
                    parameter_category: buf[0],
                    parameter_number: buf[1],
                    process_type: buf[2],
                    background_process: buf[3],
                    analysis_process: buf[4],
                    hours: read_as!(u16, buf, 5),
                    minutes: buf[7],
                    forecast_time: match buf[8] {
                        0 => Duration::minutes(read_as!(u32, buf, 9) as i64),
                        1 => Duration::hours(read_as!(u32, buf, 9) as i64),
                        2 => Duration::days(read_as!(u32, buf, 9) as i64),
                        3 => Duration::days(30 * read_as!(u32, buf, 9) as i64),
                        4 => Duration::days(365 * read_as!(u32, buf, 9) as i64),
                        5 => Duration::days(10 * 365 * read_as!(u32, buf, 9) as i64),
                        6 => Duration::days(30 * 365 * read_as!(u32, buf, 9) as i64),
                        7 => Duration::days(100 * 365 * read_as!(u32, buf, 9) as i64),
                        10 => Duration::hours(3 * read_as!(u32, buf, 9) as i64),
                        11 => Duration::hours(6 * read_as!(u32, buf, 9) as i64),
                        12 => Duration::hours(12 * read_as!(u32, buf, 9) as i64),
                        13 => Duration::seconds(read_as!(u32, buf, 9) as i64),
                        n => {
                            return Err(GribError::ParseError(format!("Forecast Time Unit `{}` does not exist.", n)))
                        },
                    },
                    first_surface: Surface {
                        surface_type: buf[13],
                        scale_factor: buf[14],
                        scaled_value: read_as!(u32, buf, 15)
                    },
                    second_surface: Surface {
                        surface_type: buf[19],
                        scale_factor: buf[20],
                        scaled_value: read_as!(u32, buf, 21)
                    }
                }))
            },
            _ => {
                Ok(Product::Unknown(buf))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Product0 {
    pub(crate) parameter_category: u8,
    pub(crate) parameter_number: u8,
    process_type: u8,
    background_process: u8,
    analysis_process: u8,
    hours: u16,
    minutes: u8,
    forecast_time: Duration,
    pub(crate) first_surface: Surface,
    second_surface: Surface,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Surface {
    pub surface_type: u8,
    pub scale_factor: u8,
    pub scaled_value: u32,
}