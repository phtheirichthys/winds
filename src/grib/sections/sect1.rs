use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identification {
    /// Identification of originating/generating centre (see Common Code Table C-1)
    pub centre_id: u16,
    /// Identification of originating/generating sub-centre (allocated by originating/ generating centre)
    pub subcentre_id: u16,
    /// GRIB Master Tables Version Number (see Code Table 1.0)
    pub master_table_version: u8,
    /// GRIB Local Tables Version Number (see Code Table 1.1)
    pub local_table_version: u8,
    /// Significance of Reference Time (see Code Table 1.2)
    pub ref_time_significance: u8,
    /// Reference time of data
    pub ref_time: DateTime<Utc>,
    /// Production status of processed data in this GRIB message
    /// (see Code Table 1.3)
    pub prod_status: u8,
    /// Type of processed data in this GRIB message (see Code Table 1.4)
    pub data_type: u8,
}
