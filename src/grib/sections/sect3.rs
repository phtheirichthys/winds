use crate::read_as;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GridDefinition {
    pub source: u8,
    /// Number of data points
    pub num_points: usize,
    pub optional_num_list_size: usize,
    pub optional_num_list_interpretation: u8,
    /// Grid Definition Template Number
    pub template_number: u16,
    pub grid: Grid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Grid {
    Grid0(Grid0),
    Unknown(Vec<u8>),
}

impl Grid {
    pub(crate) fn from_template(template_number: u16, buf: Vec<u8>) -> crate::grib::Result<Self> {

        match template_number {
            0 => {
                Ok(Grid::Grid0(Grid0 {
                    header: GridHeader {
                        earth_shape: buf[0],
                        spherical_radius: ScaledValue { scale: buf[1], value: read_as!(u32, buf, 2) },
                        major_axis: ScaledValue { scale: buf[6], value: read_as!(u32, buf, 7) },
                        minor_axis: ScaledValue { scale: buf[11], value: read_as!(u32, buf, 12) },
                    },
                    n_i: read_as!(u32, buf, 16),
                    n_j: read_as!(u32, buf, 20),
                    initial_prod_basic_angle: BasicAngle { basic_angle: read_as!(u32, buf, 24), basic_angle_sub: read_as!(u32, buf, 28) },
                    la1: read_as!(i32, buf, 32),
                    lo1: read_as!(i32, buf, 36),
                    resolution_and_component_flags: buf[40],
                    la2: read_as!(i32, buf, 41),
                    lo2: read_as!(i32, buf, 45),
                    d_i: read_as!(u32, buf, 49),
                    d_j: read_as!(u32, buf, 53),
                    scanning_mode: buf[57]
                }))
            },
            _ => {
                Ok(Grid::Unknown(buf))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScaledValue {
    scale: u8,
    value: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BasicAngle {
    basic_angle: u32,
    basic_angle_sub: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GridHeader {
    earth_shape: u8,
    spherical_radius: ScaledValue,
    major_axis: ScaledValue,
    minor_axis: ScaledValue,
}

///Grid0 Definition Template 3.0: Latitude/longitude (or equidistant cylindrical, or Plate Carree)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grid0 {
    header: GridHeader,
    pub(crate) n_i: u32,
    pub(crate) n_j: u32,
    initial_prod_basic_angle: BasicAngle,
    pub(crate) la1: i32,
    pub(crate) lo1: i32,
    resolution_and_component_flags: u8,
    la2: i32,
    lo2: i32,
    pub(crate) d_i: u32,
    pub(crate) d_j: u32,
    scanning_mode: u8,
}
