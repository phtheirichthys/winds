pub(crate) mod noaa;
mod json;

use std::cmp::Ordering;
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, Read, Seek};
use std::ops::{Neg, Sub};
use std::path::{PathBuf};
use std::sync::Arc;
use async_process::Command;
use tempfile::NamedTempFile;
use tokio::{self, time};
use tokio::sync::{RwLock};
use crate::config::{MeteofranceProviderConfig, NoaaProviderConfig, ProviderConfig, Storage};
use crate::error;
use cached::proc_macro::cached;
use crate::grib;

use crate::error::{Error, Result};
use crate::grib::sections::sect3::Grid;
use crate::grib::sections::sect4::{Product, Surface};
use crate::providers::noaa::Noaa;
use crate::stamp::{ForecastTime, ForecastTimeSpec, RefTime, Stamp};

pub struct Wind {
  lat0: f64,
  lon0: f64,
  delta_lat: f64,
  delta_lon: f64,
  n_lat: usize,
  n_lon: usize,
  u: Box<[Box<[f64]>]>,
  v: Box<[Box<[f64]>]>,
}

pub async fn start_provider(provider_config: &ProviderConfig) -> Result<Option<Winds>> {

  match provider_config {
    ProviderConfig::Noaa(NoaaProviderConfig { enabled: false, .. }) => {
      Ok(None)
    },
    ProviderConfig::Noaa(config) => {

      let noaa = Noaa::new(config)?;
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
        Some(json::load_all(self.jsons_dir(), status.forecasts.get(&keys[0]).unwrap()).await?),
        None,
        0.0
        ))
    }

    for (i, key) in keys.iter().enumerate() {
      if key.gt(&datetime) {
        let h = keys[i-1].sub(datetime).num_seconds() as f64;
        let delta = keys[i-1].sub(keys[i]).num_seconds() as f64;
        return Ok((
          Some(json::load_all(self.jsons_dir(), status.forecasts.get(&keys[i-1]).unwrap()).await?),
          Some(json::load_all(self.jsons_dir(), status.forecasts.get(&keys[i]).unwrap()).await?),
          h / delta
        ));
      }
    }

    Ok((
      Some(json::load_all(self.jsons_dir(), status.forecasts.get(keys.last().ok_or(error::Error::Error())?).unwrap()).await?),
      None,
      0.0
    ))
  }

}


async fn load_gribs(grib_dir: PathBuf, stamps: &Vec<Stamp>) -> Result<Vec<()>> {

  let mut res = Vec::new();
  for stamp in stamps {
    res.push(load_grib(grib_dir.join(stamp.file_name())).await?);
  }

  Ok(res)
}

async fn load_grib(grib_filename: PathBuf) -> Result<()> {

  let f = File::open(grib_filename)?;
  let f = BufReader::new(f);

  let grib = grib::from_reader(f)?;

  for message in grib.messages {
    let discipline = message.indicator.discipline;

    debug!("Grib : {:?} {:?}", discipline, message.product_definition);
    let data = message.decode()?;

    if message.indicator.discipline == 0 {
      match (&message.product_definition.product, &message.grid_definition.grid) {
        (Product::Product0(product), Grid::Grid0(grid)) => {
          if product.parameter_category == 2 && product.first_surface == (Surface {
            surface_type: 103,
            scale_factor: 0,
            scaled_value: 10
          }) {

            let lat0 = grid.la1;
            let lon0 = grid.lo1;
            let delta_lat = grid.d_j;
            let delta_lon = grid.d_i;
            let nb_lat = grid.n_j as usize;
            let nb_lon = grid.n_i as usize;

            match product.parameter_number {
              2 => {
                let data = build_grid(message.decode()?, nb_lat, nb_lon);
                debug!("U ok")
              },
              3 => {
                let data = build_grid(message.decode()?, nb_lat, nb_lon);
                debug!("V ok")
              }
              _ => {}
            }

          }
        }
        _ => {}
      }
    }
  }



  // let grib2 = grib::from_reader(f)?;
  //
  // for (index, message) in grib2.iter().enumerate() {
  //
  //
  //   let discipline = message.indicator().discipline;
  //   let category = message.prod_def().parameter_category();
  //   let surfaces =  message.prod_def().fixed_surfaces();
  //
  //   debug!("Grib : {:?} {:?} {:?}", discipline, category, surfaces);
  //
  //   if message.indicator().discipline == 0 && message.prod_def().parameter_category() == Some(2) && message.prod_def().fixed_surfaces().map(|(first, second)| first) == Some((FixedSurface {
  //       surface_type: 103,
  //       scale_factor: 0,
  //       scaled_value: 10 })) {
  //
  //     //TODO : use data provided by section 3
  //     let lat0 = -80.;
  //     let lon0 = 0.;
  //     let delta_lat = 1;
  //     let delta_lon = 1;
  //     let nb_lat = 181;
  //     let nb_lon = 360;
  //
  //     debug!("Message : {:?}", message.prod_def());
  //
  //     match message.prod_def().parameter_number() {
  //       Some(2) => {
  //         //let data = build_grid(grib2.get_values(index)?, nb_lat, nb_lon);
  //         debug!("U ok")
  //       },
  //       Some(3) => {
  //         //let data = build_grid(grib2.get_values(index)?, nb_lat, nb_lon);
  //         debug!("V ok")
  //       },
  //       _ => {}
  //     }
  //   }
  // }

  Ok(())
}

mod test {
  use anyhow::Result;
  use std::path::PathBuf;
  use crate::config::NoaaProviderConfig;
  use crate::providers::{load_grib, Provider};
  use crate::providers::noaa::Noaa;

  #[tokio::test]
  async fn load_grib_test() -> Result<()>{

    std::env::var("RUST_LOG").map_err(|_| {
      std::env::set_var("RUST_LOG", "error,winds=debug");
    }).unwrap_or_default();
    env_logger::init();

    let noaa = Noaa::new(&NoaaProviderConfig {
      enabled: true,
      init: None,
      gribs_dir: "data/noaa/gribs".to_string(),
      jsons_dir: "data/noaa/jsons".to_string(),
      jsons: vec![]
    })?;

    noaa.load(false, true).await?;
    tokio::spawn(async move {
      noaa.start_refresh().await;
    }).await;

    Ok(())
  }
}

fn build_grid(data: Box<[f64]>, nb_lat: usize, nb_lon: usize) -> Box<[Box<[f64]>]> {

  let is_continuous = true;

  let nb_lon = if is_continuous { nb_lon + 1 } else { nb_lon };

  let mut grid = Vec::with_capacity(nb_lat); //vec![vec![0f64; nb_lon]; nb_lat];

  let mut p = 0;
  for j in 0..nb_lat {
    let mut raw = Vec::with_capacity(nb_lon);
    for i in 0..nb_lon-1 {
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
pub(crate) trait Provider {

  fn id(&self) -> String;

  fn gribs_dir(&self) -> PathBuf;

  fn jsons_dir(&self) -> PathBuf;

  fn jsons_storages(&self) -> Vec<Storage>;

  fn max_forecast_hour(&self) -> u16;

  fn step(&self) -> u16;

  fn status(&self) -> Winds;

  fn next_update_time(&self) -> DateTime<Utc>;

  fn current_ref_time(&self) -> RefTime;

  async fn load(&self, delete: bool, load: bool) -> Result<Winds> {
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
          for storage in self.jsons_storages() {
            storage.remove(stamp.file_name()).await?;
          }
          continue;
        }
      }

      debug!("Keep `{}` {}", stamp, stamp.file_name());
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
    info!("{} - Refresh provider", self.id());

    {
      let status = self.status();
      let mut status = status.write().await;

      // Remove forecasts for which files were deleted
      status.forecasts.retain(|_, stamps| {
        for stamp in stamps {
          if !self.jsons_dir().join(stamp.file_name()).exists() {
            return false;
          }
        }
        true
      });
    }

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
    while let Some(mut stamp) = stamps.next() {

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

        for to in self.jsons_storages() {
          to.save(&json_path, stamp.file_name()).await?;
        }

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
          match fs::remove_file(self.gribs_dir().join(stamp.file_name())) {
            Ok(()) => {},
            Err(e) => error!("Error removing file {} : {}", stamp.file_name(), e),
          }
          for storage in self.jsons_storages() {
            match storage.remove(stamp.file_name()).await {
              Ok(()) => {},
              Err(e) => error!("{} - Error removing file {} from storage {} : {}", self.id(), stamp.file_name(), storage, e),
            }
          }
        }).await;
      }
    }

    self.status().set_last(stamp.ref_time, stamp.forecast_hour(), self.max_forecast_hour()).await;

    let mut stamp = stamp;
    if load {
      stamp.wind  = Some(json::load(self.jsons_dir().join(&stamp.file_name())).await?);
    }

    self.status().add_forecast(stamp).await;

    debug!("{} - Status : {}", self.id(), self.status().read().await);

    Ok(())
  }

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
        for storage in self.jsons_storages() {
          match storage.remove(stamp.file_name()).await {
            Ok(()) => {},
            Err(e) => error!("{} - Error removing file {} from storage {} : {}", self.id(), stamp.file_name(), storage, e),
          }
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

  async fn remove_forecast<F, T>(&self, forecast_time: &ForecastTime, remove: F) where F: Sync + Send + Fn(Stamp) -> T, T: Future<Output = ()> + Send;

  async fn set_last(&self, ref_time: DateTime<Utc>, forecast_time: u16, max_forecast_time: u16);

  async fn contains_key(&self, forecast_time: &ForecastTime) -> bool;
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
}

pub(crate) struct Status {
  pub provider: String,
  pub(crate) provider_name: String,
  pub(crate) current_ref_time: RefTime,
  pub(crate) last: Option<Stamp>,
  pub(crate) progress: u8,
  pub(crate) forecasts: BTreeMap<ForecastTime, Vec<Stamp>>,
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