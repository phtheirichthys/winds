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

pub(crate) struct Noaa {
    pub(crate) status: Winds,
    gribs_dir: PathBuf,
    jsons: Vec<Storage>,
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

    pub(crate) fn new(config: &NoaaProviderConfig) -> Result<Self> {
        let gribs_dir: PathBuf = config.gribs_dir.clone().into();
        Self::create_dir(&gribs_dir);

        for dir in &config.jsons {
            match dir {
                Storage::Local{dir} => Self::create_dir(&dir.into()),
                _ => {}
            }
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
            gribs_dir,
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

            if !self.gribs_dir().join(stamp.file_name()).exists() {

                match self.download_grib(&stamp).await {
                    Ok(()) => {
                        something_new = true;
                        self.on_stamp_downloaded(stamp).await;
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

        let client = reqwest::Client::new();
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
                                //std::fs::rename(path, self.gribs_dir().join(stamp.file_name()))?;
                                std::fs::copy(&path, self.gribs_dir().join(stamp.file_name()))?;
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

    fn gribs_dir(&self) -> PathBuf {
        self.gribs_dir.clone()
    }

    fn jsons_storages(&self) -> Vec<Storage> {
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

    async fn on_stamp_downloaded(&self, stamp: Stamp) {

        if self.status.contains_key(&stamp.forecast_time).await && stamp.forecast_hour() > 6 { // keep previous forecast to merge
            self.status.remove_forecast(&stamp.forecast_time, async move |stamp| {
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

        self.status().set_last(stamp.ref_time, stamp.forecast_hour(), self.max_forecast_hour()).await;
        self.status().add_forecast(stamp).await;

        debug!("{} - Status : {:?}", self.id(), self.status.read().await);
    }
}
