use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use crate::providers::Wind;

pub(crate) type RefTime = DateTime<Utc>;

impl RefTimeSpec for RefTime {}

pub(crate) trait RefTimeSpec {
    fn new(time: DateTime<Utc>) -> RefTime {
        time.duration_trunc(6.hours()).expect("now truncated by 6 hours")
    }

    fn now() -> RefTime {
        Self::new(Utc::now())
    }
}

pub type ForecastTime = DateTime<Utc>;

impl ForecastTimeSpec for ForecastTime {
    fn from_now(&self) -> Duration {
        *self - Utc::now()
    }
}

pub(crate) trait ForecastTimeSpec {
    fn from_ref_time(ref_time: &RefTime, h: u16) -> ForecastTime {
        *ref_time + h.hours()
    }

    fn from_now(&self) -> Duration;
}

impl Durations for u16 {
    fn hours(&self) -> Duration {
        chrono::Duration::hours(*self as i64)
    }
}

pub(crate) trait Durations {
    fn hours(&self) -> chrono::Duration;
}


pub struct Stamp {
    pub ref_time: RefTime,
    pub forecast_time: ForecastTime,
    pub(crate) wind: Option<Arc<Wind>>,
}

impl Stamp {
    pub(crate) fn from_now(&self) -> Duration {
        self.forecast_time - Utc::now()
    }

    pub(crate) fn forecast_hour(&self) -> u16 {
        (self.forecast_time - self.ref_time).num_hours() as u16
    }

    pub(crate) fn file_name(&self) -> String {
        format!("{}.f{:03}", self.ref_time.format("%Y%m%d%H"), self.forecast_hour())
    }

}

impl Display for Stamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}Z+{:03}", self.ref_time.format("%H"), self.forecast_hour())
    }
}

impl TryFrom<&String> for Stamp {
    type Error = StampError;

    fn try_from(filename: &String) -> Result<Self, Self::Error> {
        match filename.split('.').collect::<Vec<&str>>()[..] {
            [date, hour] => {
                let ref_time = Utc.datetime_from_str((String::from(date) + "00").as_str(), "%Y%m%d%H%M")?;
                let forecast_hour = hour[1..4].parse::<u16>()?;

                let res = Self {
                    ref_time,
                    forecast_time: ref_time + forecast_hour.hours(),
                    wind: None
                };

                Ok(res)
            },
            _ => {
                Err(StampError::FilenameError(filename.clone()))
            }
        }
    }
}

impl TryFrom<&PathBuf> for Stamp {
    type Error = StampError;

    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let filename = &path.file_name().expect("the file name").to_string_lossy().to_string();

        match filename.try_into() {
            Ok(stamp) => Ok(stamp),
            Err(_) => Err(StampError::FilenameError(filename.clone()))
        }
    }
}

impl From<(&RefTime, ForecastTime)> for Stamp {
    fn from((ref_time, forecast_time): (&RefTime, ForecastTime)) -> Self {
        Self {
            ref_time: ref_time.clone(),
            forecast_time,
            wind: None,
        }
    }
}

impl From<(&RefTime, u16)> for Stamp {
    fn from((ref_time, h): (&RefTime, u16)) -> Self {
        Self {
            ref_time: ref_time.clone(),
            forecast_time: *ref_time + Duration::hours(h as i64),
            wind: None,
        }
    }
}


#[derive(thiserror::Error, Debug)]
pub enum StampError {
    #[error("Wrong filename format `{0}`")]
    FilenameError(String),

    #[error("ParseError: {0}")]
    ParseError(#[from] chrono::ParseError),

    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
}