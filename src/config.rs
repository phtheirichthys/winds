use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Config {
  pub(crate) providers: Vec<ProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ProviderConfig {
  Noaa(NoaaProviderConfig),
  Meteofrance(MeteofranceProviderConfig)
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub(crate) struct NoaaProviderConfig {
  pub(crate) enabled: bool,
  pub(crate) init: Option<DateTime<Utc>>,
  pub(crate) gribs_dir: String,
  pub(crate) jsons_dir: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub(crate) struct MeteofranceProviderConfig {
  pub(crate) enabled: bool,
  token: String,
}