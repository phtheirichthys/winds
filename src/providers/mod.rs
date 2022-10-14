pub mod noaa;
pub(crate) mod json;
pub mod zezo;

use std::cmp::Ordering;
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::ops::Sub;
use std::path::{PathBuf};
use std::sync::Arc;
use anyhow::anyhow;
use async_process::Command;
use tempfile::NamedTempFile;
use tokio::{self, time};
use tokio::sync::{RwLock};
use crate::config::{MeteofranceProviderConfig, NoaaProviderConfig, ProviderConfig, Storage, ZezoProviderConfig};
use crate::error;

use crate::error::{Error, Result};
use crate::providers::json::Message;
use crate::providers::noaa::Noaa;
use crate::providers::zezo::Zezo;
use crate::stamp::{ForecastTime, ForecastTimeSpec, RefTime, Stamp};

pub struct Wind {
  pub lat0: f64,
  pub lon0: f64,
  pub delta_lat: f64,
  pub delta_lon: f64,
  n_lat: usize,
  n_lon: usize,
  pub u: Box<[Box<[f64]>]>,
  pub v: Box<[Box<[f64]>]>,
}

impl Debug for Wind {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Wind")
        .field("lat0", &self.lat0)
        .field("lon0", &self.lon0)
        .field("delta_lat", &self.delta_lat)
        .field("delta_lon", &self.delta_lon)
        .field("n_lat", &self.n_lat)
        .field("n_lon", &self.n_lon)
        .finish()
  }
}

impl TryFrom<Vec<Message>> for Wind {
  type Error = anyhow::Error;

  fn try_from(messages: Vec<Message>) -> std::result::Result<Self, Self::Error> {
    let mut wind = None;

    for message in messages {
      if message.header.discipline == 0 {
        if message.header.parameter_category == 2 && message.header.surface1_type == 103 && message.header.surface1_value == 10.0 {

          let lat0 = message.header.la1;
          let lon0 = message.header.lo1;
          let delta_lat = message.header.dy;
          let delta_lon = message.header.dx;
          let n_lat = message.header.ny;
          let n_lon = message.header.nx;

          match message.header.parameter_number {
            2 => {
              let u = build_grid(message.data, n_lat, n_lon);

              let mut wind = wind.get_or_insert(Wind {
                lat0,
                lon0,
                delta_lat,
                delta_lon,
                n_lat,
                n_lon,
                u: Vec::new().into_boxed_slice(),
                v: Vec::new().into_boxed_slice(),
              });

              wind.u = u;
            },
            3 => {
              let v = build_grid(message.data, n_lat, n_lon);

              let mut wind = wind.get_or_insert(Wind {
                lat0,
                lon0,
                delta_lat,
                delta_lon,
                n_lat,
                n_lon,
                u: Vec::new().into_boxed_slice(),
                v: Vec::new().into_boxed_slice(),
              });

              wind.v = v;
            }
            _ => {}
          }

        }
      }
    }

    wind.ok_or(anyhow!("Error loading wind from messages"))
  }
}

pub async fn start_provider(provider_config: &ProviderConfig) -> Result<Option<Winds>> {

  match provider_config {
    ProviderConfig::Noaa(NoaaProviderConfig { enabled: false, .. }) => {
      Ok(None)
    },
    ProviderConfig::Noaa(config) => {

      let noaa = Noaa::from_config(config)?;
      let winds = noaa.load(true, false).await?;
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
    ProviderConfig::Zezo(ZezoProviderConfig { enabled: false, .. }) => {
      Ok(None)
    },
    ProviderConfig::Zezo(config) => {

      let zezo = Zezo::from_config(config)?;
      let winds = zezo.load(true, false).await?;
      zezo.init(config.init).await;
      tokio::spawn(async move {
        zezo.start().await;
      });
      Ok(Some(winds))
    },
  }
}

#[async_trait]
pub trait JsonProvider {
  fn jsons_dir(&self) -> PathBuf;

  async fn find_winds(&self, datetime: DateTime<Utc>) -> Result<(Option<Vec<Wind>>, Option<Vec<Wind>>, f64)>;
}

#[async_trait]
impl JsonProvider for dyn Provider + Sync {
  fn jsons_dir(&self) -> PathBuf {

    self.jsons_dir()
  }

  async fn find_winds(&self, datetime: DateTime<Utc>) -> Result<(Option<Vec<Wind>>, Option<Vec<Wind>>, f64)> {

    let status = self.status().clone();
    let status = status.read().await;

    let keys: Vec<_> = status.forecasts.keys().cloned().collect();

    if keys[0] > datetime {
      return Ok((
        Some(json::load_all(status.forecasts.get(&keys[0]).unwrap()).await?),
        None,
        0.0
        ))
    }

    for (i, key) in keys.iter().enumerate() {
      if key.gt(&datetime) {
        let h = keys[i-1].sub(datetime).num_seconds() as f64;
        let delta = keys[i-1].sub(keys[i]).num_seconds() as f64;
        return Ok((
          Some(json::load_all(status.forecasts.get(&keys[i-1]).unwrap()).await?),
          Some(json::load_all(status.forecasts.get(&keys[i]).unwrap()).await?),
          h / delta
        ));
      }
    }

    Ok((
      Some(json::load_all(status.forecasts.get(keys.last().ok_or(error::Error::Error())?).unwrap()).await?),
      None,
      0.0
    ))
  }

}

fn build_grid(data: Box<[f64]>, nb_lat: usize, nb_lon: usize) -> Box<[Box<[f64]>]> {

  let is_continuous = true;

  let size = if is_continuous { nb_lon + 1 } else { nb_lon };

  let mut grid = Vec::with_capacity(nb_lat); //vec![vec![0f64; nb_lon]; nb_lat];

  let mut p = 0;
  for _ in 0..nb_lat {
    let mut raw = Vec::with_capacity(size);
    for _ in 0..nb_lon {
      raw.push(data[p]);
      p += 1;
    }
    if is_continuous {
      raw.push(raw[0]);
    }
    grid.push(raw.into_boxed_slice());
  }

  grid.into_boxed_slice()
}

#[async_trait]
pub trait Provider {

  fn id(&self) -> String;

  fn jsons_storage(&self) -> Storage;

  fn max_forecast_hour(&self) -> u16;

  fn step(&self) -> u16;

  fn status(&self) -> Winds;

  fn next_update_time(&self) -> DateTime<Utc>;

  fn current_ref_time(&self) -> RefTime;

  async fn load(&self, delete: bool, load: bool) -> Result<Winds> {
    info!("{} - Load provider", self.id());

    let mut stamps = self.jsons_storage().list().await?;

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

      if delete {
        if let Some(next_stamp) = stamps.peek() {
          if next_stamp.from_now() < chrono::Duration::zero() {
            info!("{} - Delete `{}` {}", self.id(), stamp, stamp.file_name());
            self.jsons_storage().remove(stamp.file_name()).await?;
            continue;
          }
        }

        debug!("Keep `{}` {}", stamp, stamp.file_name());
      }

      match self.on_stamp_downloaded(delete, load, stamp).await {
        Err(e) => {
          error!("Error executing downloaded callback : {:?}", e);
        },
        Ok(_) => {}
      }
    }

    if let Some(last) = self.status().get_last().await {
      info!("{} - `{}Z+{:03}` : {}%", self.id(), last.ref_time.format("%H"), last.forecast_hour(), self.status().get_progress().await);
    }

    Ok(self.status())
  }

  async fn refresh(&self) -> Result<()> {
    debug!("{} - Refresh provider", self.id());

    {
      let status = self.status();
      let mut status = status.write().await;

      // Remove forecasts for which files were deleted
      let storage = self.jsons_storage().clone();
      status.forecasts.retain(|_, stamps| {
        for stamp in stamps {
          if !storage.exists_blocking(stamp.file_name()).unwrap_or(false) {
            return false;
          }
        }
        true
      });
    }

    let mut stamps = self.jsons_storage().list().await?;

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

      if !self.status().contains_key(&stamp.forecast_time).await {
        debug!("Add {} to existing forecast {}", &stamp, &stamp.forecast_time);
        self.on_stamp_downloaded(false, true, stamp).await;
      } else if stamp.forecast_hour() == 0 {
        debug!("Add {} to new forecast {}", &stamp, &stamp.forecast_time);
        self.on_stamp_downloaded(false, true, stamp).await;
      }

    }

    Ok(())
  }

  async fn start_refresh(&self) {
    info!("{} - Start provider", self.id());

    loop {
      self.refresh().await;
      tokio::time::sleep(time::Duration::from_secs(10)).await;
    }
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

  async fn on_file_downloaded(&self, grib_path: PathBuf, stamp: &Stamp) -> Result<()> {
    debug!("{} - Convert grib `{}` to json", self.id(), stamp);

    let file = NamedTempFile::new()?;
    let (_, json_path) = file.into_parts();

    let output = Command::new("grib2json/bin/grib2json")
        .arg("--data")
        .arg("--names")
        .arg("--fs")
        .arg("103")
        .arg("--fv")
        .arg("10")
        .arg("--compact")
        .arg("--output")
        .arg(&json_path)
        .arg(grib_path)
        .output().await?;

    debug!("{} - {}", self.id(), String::from_utf8_lossy(output.stdout.as_slice()));
    match output.status.exit_ok() {
      Ok(()) => {

        self.jsons_storage().save(&json_path, stamp.file_name()).await?;

        std::fs::remove_file(&json_path).unwrap_or_default();

        Ok(())
      }
      Err(e) => {
        error!("{} - Error converting grib `{}` to json : {}", self.id(), stamp, String::from_utf8_lossy(output.stderr.as_slice()));
        Err(Error::ExitStatusError(e))
      }
    }
  }

  async fn on_stamp_downloaded(&self, delete: bool, load: bool, stamp: Stamp) -> Result<()> {

    if delete {
      if self.status().contains_key(&stamp.forecast_time).await && stamp.forecast_hour() > 6 { // keep previous forecast to merge
        self.status().remove_forecast(&stamp.forecast_time, async move |stamp| {
          info!("Delete `{}`", stamp);
          match self.jsons_storage().remove(stamp.file_name()).await {
            Ok(()) => {},
            Err(e) => error!("{} - Error removing file {} from storage {} : {}", self.id(), stamp.file_name(), self.jsons_storage(), e),
          }
        }).await;
      }
    }

    self.status().set_last(stamp.ref_time, stamp.forecast_hour(), self.max_forecast_hour()).await;

    let mut stamp = stamp;
    if load {
      debug!("Load `{}` {}", stamp, stamp.file_name());
      stamp.wind  = Some(Arc::new(self.load_stamp(&stamp).await?.try_into()?));
    }

    self.status().add_forecast(stamp).await;

    debug!("{} - Status : {}", self.id(), self.status().read().await);

    Ok(())
  }

  async fn load_stamp(&self, stamp: &Stamp) -> Result<Wind>;

  async fn clean(&self) {

    let status = self.status();
    let mut status = status.write().await;

    while let Some((_, stamps)) = status.forecasts.drain_filter(|forecast, _| forecast.from_now() < Duration::hours(-3)).next() {
      for stamp in stamps {
        info!("{} - Delete {}", self.id(), stamp);
        match self.jsons_storage().remove(stamp.file_name()).await {
          Ok(()) => {},
          Err(e) => error!("{} - Error removing file {} from storage {} : {}", self.id(), stamp.file_name(), self.jsons_storage(), e),
        }
      }
    }
  }

}

pub type Winds = Arc<RwLock<Status>>;

#[async_trait]
pub trait WindsSpec {
  async fn get_last(&self) -> Option<Stamp>;

  async fn get_progress(&self) -> u8;

  async fn set_current_ref_time(&self, ref_time: DateTime<Utc>);

  async fn add_forecast(&self, forecast: Stamp);

  async fn remove_forecast<F, T>(&self, forecast_time: &ForecastTime, remove: F) where F: Sync + Send + Fn(Stamp) -> T, T: Future<Output = ()> + Send;

  async fn set_last(&self, ref_time: DateTime<Utc>, forecast_time: u16, max_forecast_time: u16);

  async fn contains_key(&self, forecast_time: &ForecastTime) -> bool;

  async fn find(&self, m: &DateTime<Utc>) -> (Vec<Arc<Wind>>, Option<Vec<Arc<Wind>>>, f64);
}

#[async_trait]
impl WindsSpec for Winds {
  async fn get_last(&self) -> Option<Stamp> {
    let it = self.read().await;

    it.last.as_ref().map(|l| Stamp {
      ref_time: l.ref_time,
      forecast_time: l.forecast_time,
      wind: None,
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

  async fn remove_forecast<F, T>(&self, forecast_time: &ForecastTime, remove: F) where F: Sync + Send + Fn(Stamp) -> T, T: Future<Output = ()> + Send {

    let mut it = self.write().await;

    if let Some(stamps) = it.forecasts.remove(forecast_time) {
      for stamp in stamps {
        remove(stamp).await;
      }
    }
  }

  async fn set_last(&self, ref_time: DateTime<Utc>, forecast_time: u16, max_forecast_time: u16) {
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

  async fn find(&self, m: &DateTime<Utc>) -> (Vec<Arc<Wind>>, Option<Vec<Arc<Wind>>>, f64) {
    let status = self.read().await;

    let mut previous: Option<(&ForecastTime, &Vec<Stamp>)> = None;
    for (_, (forecast_time, stamps)) in status.forecasts.iter().enumerate() {
      if forecast_time > m {
        match previous {
          None => {
            let w1 = stamps.iter().map_while(|s| {
              match &s.wind {
                None => { None }
                Some(s) => { Some(s.clone()) }
              }
            }).collect();
            return (w1, None, 0.0);
          }
          Some((previous_forecast_time, previous_stamps)) => {
            let h = (m.clone() - previous_forecast_time.clone()).num_minutes();
            let delta = (forecast_time.clone() - previous_forecast_time.clone()).num_minutes();
            let w1 = previous_stamps.iter().map_while(|s| match &s.wind {
              None => { None }
              Some(s) => { Some(s.clone()) }
            }).collect();
            if h == 0 {
              return (w1, None, 0.0);
            }
            let w2 = stamps.iter().map_while(|s| match &s.wind {
              None => { None }
              Some(s) => { Some(s.clone()) }
            }).collect();
            return (w1, Some(w2), h as f64 / delta as f64);
          }
        }
      }

      previous = Some((forecast_time, stamps));
    }

    let (_, previous_stamps) = previous.unwrap();
    let w1 = previous_stamps.iter().map_while(|s| match &s.wind {
      None => { None }
      Some(s) => { Some(s.clone()) }
    }).collect();

    (w1, None, 0.0)
  }
}

pub struct Status {
  pub(crate) provider: String,
  pub(crate) provider_name: String,
  pub(crate) current_ref_time: RefTime,
  pub(crate) last: Option<Stamp>,
  pub(crate) progress: u8,
  pub forecasts: BTreeMap<ForecastTime, Vec<Stamp>>,
}

impl Display for Status {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match &self.last {
      Some(last) => {
        write!(f, "{} - `{}Z+{:03}` : {}%", &self.provider, last.ref_time.format("%H"), last.forecast_hour(), &self.progress)
      }
      None => {
        write!(f, "{} : {}%", &self.provider, &self.progress)
      }
    }
  }
}
