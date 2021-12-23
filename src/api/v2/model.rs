use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use crate::providers::Status;

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct Forecasts {
    provider: String,
    provider_name: String,
    current_ref_time: DateTime<Utc>,
    last: Option<LastForecast>,
    progress: u8,
    forecasts: Vec<Forecast>,
}

#[derive(Deserialize, Serialize, Debug)]
struct LastForecast {
    forecast_time: DateTime<Utc>,
    ref_time: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Forecast {
    forecast_time: DateTime<Utc>,
    ref_times: Vec<DateTime<Utc>>,
}

impl From<&Status> for Forecasts {
    fn from(forecasts: &Status) -> Self {
        Forecasts {
            provider: forecasts.provider.clone(),
            provider_name: forecasts.provider_name.clone(),
            current_ref_time: forecasts.current_ref_time,
            last: forecasts.last.as_ref().map(|last| LastForecast { forecast_time: last.forecast_time, ref_time: last.ref_time }),
            progress: forecasts.progress,
            forecasts: {
                let mut forecasts = forecasts.forecasts.iter()
                    .map(|(forecast_time, forecasts)| Forecast {
                        forecast_time: forecast_time.clone(),
                        ref_times: forecasts.iter().map(|forecast| forecast.ref_time).collect(),
                    }).collect::<Vec<Forecast>>();

                forecasts.sort_by(|a, b| a.forecast_time.cmp(&b.forecast_time));
                forecasts
            },
        }
    }
}
