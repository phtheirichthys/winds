pub(crate) mod noaa;

use std::cmp::Ordering;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use async_process::Command;
use tokio::{self, time};
use tokio::sync::{RwLock};
use crate::config::{MeteofranceProviderConfig, NoaaProviderConfig, ProviderConfig};

use crate::error::{Error, Result};
use crate::providers::noaa::Noaa;
use crate::stamp::{ForecastTime, ForecastTimeSpec, RefTime, Stamp};

pub(crate) async fn start_provider(provider_config: &ProviderConfig) -> Result<Option<Winds>> {

  match provider_config {
    ProviderConfig::Noaa(NoaaProviderConfig { enabled: false, .. }) => {
      Ok(None)
    },
    ProviderConfig::Noaa(config) => {

      let noaa = Noaa::new(config)?;
      let winds = noaa.load().await?;
      noaa.init(config.init).await;
      tokio::spawn(async move {
        noaa.start().await;
      });
      Ok(Some(winds))
    },
    ProviderConfig::Meteofrance(MeteofranceProviderConfig { enabled: false, .. }) => {
      Ok(None)
    },
    ProviderConfig::Meteofrance(_config) => {
      todo!()
    }
  }
}

#[async_trait]
pub(crate) trait Provider {

  fn id(&self) -> String;

  fn gribs_dir(&self) -> PathBuf;

  fn jsons_dir(&self) -> PathBuf;

  fn max_forecast_hour(&self) -> i64;

  fn step(&self) -> i64;

  fn status(&self) -> Winds;

  fn next_update_time(&self) -> DateTime<Utc>;

  fn current_ref_time(&self) -> RefTime;

  async fn load(&self) -> Result<Winds>{
    info!("{} - Load provider", self.id());

    let mut stamps: Vec<Stamp> = Vec::new();

    // Walk throw grib files
    let paths = fs::read_dir(self.gribs_dir())?;
    for entry in paths {
      if let Ok(entry) = entry {
        if let Ok(metadata) = entry.metadata() {
          if metadata.is_file() {
            stamps.push((&entry.path()).try_into()?);
          }
        }
      }
    }

    stamps.sort_by(|a, b| {
      match a.forecast_time.cmp(&b.forecast_time) {
        Ordering::Equal => {
          a.ref_time.cmp(&b.ref_time)
        },
        Ordering::Less | Ordering::Greater => {
          a.forecast_time.cmp(&b.forecast_time)
        }
      }
    });

    let mut stamps = stamps.into_iter().peekable();
    while let Some(stamp) = stamps.next() {

      if let Some(next_stamp) = stamps.peek() {
        if next_stamp.from_now() < chrono::Duration::zero() {
          info!("{} - Delete `{}` {}", self.id(), stamp, stamp.file_name());
          fs::remove_file(self.gribs_dir().join(stamp.file_name()))?;
          continue;
        }
      }

      debug!("Keep `{}` {}", stamp, stamp.file_name());
      self.on_stamp_downloaded(stamp).await;
    }


    if let Some(last) = self.status().get_last().await {
      info!("{} - `{}Z+{:03}` : {}%", self.id(), last.ref_time.format("%H"), last.forecast_hour(), self.status().get_progress().await);
    }

    Ok(self.status())
  }

  async fn start(&self) {
    info!("{} - Start provider", self.id());

    loop {
      self.clean().await;
      self.download().await;
      tokio::time::sleep(time::Duration::from_secs(300)).await;
    }
  }

  async fn init(&self, ref_time: Option<RefTime>) {
    if let Some(ref_time) = ref_time {
      info!("{} - Init provider with ref {}", self.id(), ref_time);
      self.download_at(ref_time).await;
    }
  }

  async fn download(&self);

  async fn download_at(&self, ref_time: RefTime);

  async fn on_file_downloaded(&self, path: PathBuf, stamp: &Stamp) -> Result<()> {
    debug!("{} - Convert grib `{}` to json", self.id(), stamp);

    let output = Command::new("grib2json/bin/grib2json")
        .arg("--data")
        .arg("--names")
        .arg("--fs")
        .arg("103")
        .arg("--fv")
        .arg("10")
        .arg("--compact")
        .arg("--output")
        .arg(self.jsons_dir().join(stamp.file_name()))
        .arg(path)
        .output().await?;

    debug!("{} - {}", self.id(), String::from_utf8_lossy(output.stdout.as_slice()));
    match output.status.exit_ok() {
      Ok(()) => {
        Ok(())
      }
      Err(e) => {
        error!("{} - Error converting grib `{}` to json : {}", self.id(), stamp, String::from_utf8_lossy(output.stderr.as_slice()));
        Err(Error::ExitStatusError(e))
      }
    }
  }

  async fn on_stamp_downloaded(&self, stamp: Stamp);

  async fn clean(&self) {

    let status = self.status();
    let mut status = status.write().await;

    while let Some((_, stamps)) = status.forecasts.drain_filter(|forecast, _| forecast.from_now() < Duration::hours(-3)).next() {
      for stamp in stamps {
        info!("{} - Delete {}", self.id(), stamp);
        match fs::remove_file(self.gribs_dir().join(stamp.file_name())) {
          Ok(()) => {},
          Err(e) => error!("{} - Error removing file {} : {}", self.id(), stamp.file_name(), e),
        }
      }
    }
  }

}

pub(crate) type Winds = Arc<RwLock<Status>>;

#[async_trait]
pub(crate) trait WindsSpec {
  async fn get_last(&self) -> Option<Stamp>;

  async fn get_progress(&self) -> u8;

  async fn set_current_ref_time(&self, ref_time: DateTime<Utc>);

  async fn add_forecast(&self, forecast: Stamp);

  async fn remove_forecast<F>(&self, forecast_time: &ForecastTime, remove: F) where F: Send + Fn(Stamp);

  async fn set_last(&self, ref_time: DateTime<Utc>, forecast_time: i64, max_forecast_time: i64);

  async fn contains_key(&self, forecast_time: &ForecastTime) -> bool;
}

#[async_trait]
impl WindsSpec for Winds {
  async fn get_last(&self) -> Option<Stamp> {
    let it = self.read().await;

    it.last.as_ref().map(|l| Stamp {
      ref_time: l.ref_time,
      forecast_time: l.forecast_time
    })
  }

  async fn get_progress(&self) -> u8 {
    self.read().await.progress
  }

  async fn set_current_ref_time(&self, ref_time: DateTime<Utc>) {
    let mut it = self.write().await;

    it.current_ref_time = ref_time;
  }

  async fn add_forecast(&self, forecast: Stamp) {
    let mut it = self.write().await;

    let files = it.forecasts.entry(forecast.forecast_time).or_insert(Vec::new());
    files.push(forecast);
  }

  async fn remove_forecast<F>(&self, forecast_time: &ForecastTime, remove: F) where F: Send + Fn(Stamp) {

    let mut it = self.write().await;

    if let Some(stamps) = it.forecasts.remove(forecast_time) {
      for stamp in stamps {
        remove(stamp);
      }
    }
  }

  async fn set_last(&self, ref_time: DateTime<Utc>, forecast_time: i64, max_forecast_time: i64) {
    let mut it = self.write().await;

    if it.last.is_none() || it.last.as_ref().unwrap().ref_time <= ref_time {
      it.last = Some((&ref_time, forecast_time).into());
      it.progress = (100 * forecast_time / max_forecast_time) as u8;
    }
  }

  async fn contains_key(&self, forecast_time: &ForecastTime) -> bool {
    let it = self.read().await;

    it.forecasts.contains_key(forecast_time)
  }


}

#[derive(Debug)]
pub(crate) struct Status {
  pub(crate) provider: String,
  pub(crate) provider_name: String,
  pub(crate) current_ref_time: RefTime,
  pub(crate) last: Option<Stamp>,
  pub(crate) progress: u8,
  pub(crate) forecasts: HashMap<ForecastTime, Vec<Stamp>>,
}
