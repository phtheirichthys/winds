use async_recursion::async_recursion;
use std::path::PathBuf;
use std::fs;
use std::io::Write;
use std::ops::Neg;
use std::sync::Arc;
use chrono::{DateTime, Duration, Utc};
use http::StatusCode;
use tempfile::NamedTempFile;
use tokio::sync::{RwLock};
use crate::config::{NoaaProviderConfig, Storage};
use crate::providers::{Provider, Status, WindsSpec, Winds};
use crate::error::{Error, Result};
use crate::stamp::{Durations, ForecastTime, ForecastTimeSpec, RefTime, RefTimeSpec, Stamp};

pub struct Noaa {
    pub(crate) status: Winds,
    jsons: Storage,
}

impl Noaa {
    fn create_dir(dir: &PathBuf) {
        if !dir.exists() {
            if let Err(e) = fs::create_dir_all(dir) {
                panic!("Error creating dir {:?} : {}", dir, e);
            }
            info!("{:?} created successfully", dir);
        } else if !dir.is_dir() {
            panic!("{:?} is not a directory", dir);
        }
    }

    pub fn new(jsons_dir: String) -> Result<Self> {
        Self::create_dir(&(&jsons_dir).into());

        Ok(Self {
            status: Arc::new(RwLock::new(Status {
                provider: "noaa".to_string(),
                provider_name: "Noaa".to_string(),
                current_ref_time: Self::current_ref_time(),
                last: None,
                progress: 0,
                forecasts: Default::default()
            })),
            jsons: Storage::Local { dir: jsons_dir },
        })

    }

    pub(crate) fn from_config(config: &NoaaProviderConfig) -> Result<Self> {
        match &config.jsons {
            Storage::Local{dir} => Self::create_dir(&dir.into()),
            _ => {}
        }

        Ok(Self {
            status: Arc::new(RwLock::new(Status {
                provider: "noaa".to_string(),
                provider_name: "Noaa".to_string(),
                current_ref_time: Self::current_ref_time(),
                last: None,
                progress: 0,
                forecasts: Default::default()
            })),
            jsons: config.jsons.clone(),
        })
    }

    fn current_ref_time() -> RefTime {
        let mut ref_time = RefTime::now();
        if Utc::now() < Self::next_update_time() {
            ref_time = ref_time - 6.hours();
        }

        ref_time
    }

    fn next_update_time() -> DateTime<Utc> {
        let ref_time = RefTime::now();

        ref_time + Duration::hours(3) + Duration::minutes(30)
    }

    async fn download_first(&self, ref_time: RefTime) -> Result<bool> {
        self.download_next(true, ref_time).await
    }

    #[async_recursion]
    async fn download_next(&self, first: bool, ref_time: RefTime) -> Result<bool> {

        let mut something_new = false;

        let mut h = 6;
        let mut first = first;

        while h <= self.max_forecast_hour() {
            let forecast_time = ForecastTime::from_ref_time(&ref_time, h);

            if forecast_time.from_now() <= self.step().hours().neg() {
                h += self.step();
                continue;
            }

            let stamp: Stamp = (&ref_time, forecast_time).into();

            if !self.jsons.exists(stamp.file_name()).await? {

                match self.download_grib(&stamp).await {
                    Ok(()) => {
                        something_new = true;
                        self.on_stamp_downloaded(true, false, stamp).await;
                    },
                    Err(Error::StampNotFoundError()) => {
                        if first {
                            return self.download_next(false, (ref_time - 6.hours()).into()).await;
                        }
                        break;
                    }
                    Err(e) => {
                        error!("Error downloading grib `{}` : {:?}", stamp, e);
                        break;
                    }
                }
            }

            h += self.step();
            first = false;
        }

        Ok(something_new)
    }

    async fn download_grib(&self, stamp: &Stamp) -> Result<()> {

        let url = format!("https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl");

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        let req = client.get(url).query(&[
            ("dir", format!("/gfs.{}/{}/atmos", stamp.ref_time.format("%Y%m%d"), stamp.ref_time.format("%H")).as_str()),
            ("file", format!("gfs.t{}z.pgrb2.1p00.f{:03}", stamp.ref_time.format("%H"), stamp.forecast_hour()).as_str()),
            ("lev_10_m_above_ground", "on"),
            ("var_UGRD", "on"),
            ("var_VGRD", "on"),
            ("leftlon", "0"),
            ("rightlon", "360"),
            ("toplat", "90"),
            ("bottomlat", "-90"),
        ]).build()?;

        debug!("`{}` Try to download {}", stamp, req.url());

        match client.execute(req).await {
            Ok(response) => {
                match response.status() {
                    StatusCode::OK => {
                        let file = NamedTempFile::new()?;

                        let (mut file, path) = file.into_parts();
                        file.write(response.bytes().await?.as_ref())?;

                        match self.on_file_downloaded(path.to_path_buf(), stamp).await {
                            Ok(()) => {
                                std::fs::remove_file(path).unwrap_or_default();

                                info!("`{}` Downloaded", stamp);

                                Ok(())
                            }
                            Err(e) => {
                                std::fs::remove_file(path)?;
                                Err(e)
                            }
                        }
                    },
                    StatusCode::NOT_FOUND => {
                        debug!("Download failed `{}` : {}", stamp, StatusCode::NOT_FOUND);
                        Err(Error::StampNotFoundError())
                    },
                    any => {
                        warn!("Download failed `{}` : {}", stamp, any);
                        Err(Error::Error())
                    }
                }
            },
            Err(e) => {
                error!("Error downloading grib file {} : {}", stamp, e);
                Err(Error::Error())
            }
        }
    }
}

#[async_trait]
impl Provider for Noaa {

    fn id(&self) -> String {
        String::from("noaa")
    }

    fn jsons_storage(&self) -> Storage {
        self.jsons.clone()
    }

    fn max_forecast_hour(&self) -> u16 {
        384
    }

    fn step(&self) -> u16 {
        3
    }

    fn status(&self) -> Winds {
        self.status.clone()
    }

    fn current_ref_time(&self) -> RefTime {
        Self::current_ref_time()
    }

    fn next_update_time(&self) -> DateTime<Utc> {
        Self::next_update_time()
    }

    async fn download(&self) {
        let ref_time = self.current_ref_time();
        self.download_at(ref_time).await;
    }

    async fn download_at(&self, ref_time: RefTime) {
        debug!("Is there something to download ?");

        match self.download_first(ref_time).await {
            Ok(something_new) => {
                debug!("Nothing more to download for now");
                if something_new {
                    let status = self.status();
                    let last = status.get_last().await.expect("the last");
                    info!("`{}Z+{:03}` : {}%", last.ref_time.format("%H"), last.forecast_hour(), self.status().get_progress().await);
                }
            },
            Err(e) => {
                error!("An error occurred while trying to download : {:?}", e);
            }
        }
    }

}
