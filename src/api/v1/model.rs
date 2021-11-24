use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use crate::providers::Status;

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct Forecasts {
    provider: String,
    provider_name: String,
    current_ref_time: DateTime<Utc>,
    last_forecast: Option<Forecast>,
    progress: u8,
    forecasts: HashMap<DateTime<Utc>, Vec<Forecast>>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Forecast {
    ref_time: DateTime<Utc>,
    forecast_time: DateTime<Utc>,
}

impl From<&Status> for Forecasts {
    fn from(forecasts: &Status) -> Self {
        Forecasts {
            provider: forecasts.provider.clone(),
            provider_name: forecasts.provider_name.clone(),
            current_ref_time: forecasts.current_ref_time,
            last_forecast: forecasts.last.as_ref().map(|last| Forecast { ref_time: last.ref_time, forecast_time: last.forecast_time }),
            progress: forecasts.progress,
            forecasts: forecasts.forecasts.iter()
                .map(|(forecast_time, forecasts)| (
                    forecast_time.clone(),
                    forecasts.iter().map(|forecast| Forecast { ref_time: forecast.ref_time, forecast_time: forecast.forecast_time }).collect::<Vec<Forecast>>())
                ).collect(),
        }
    }
}
