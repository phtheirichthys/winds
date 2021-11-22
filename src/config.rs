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
  enable: bool,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub(crate) struct MeteofranceProviderConfig {
  enable: bool,
  token: String,
}