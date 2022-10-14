use async_recursion::async_recursion;
use std::path::PathBuf;
use std::fs;
use std::io::Write;
use std::ops::Neg;
use std::sync::Arc;
use chrono::{DateTime, Duration, Timelike, Utc};
use http::StatusCode;
use image::GenericImageView;
use image::io::Reader as ImageReader;
use tempfile::NamedTempFile;
use tokio::sync::{RwLock};
use crate::config::{Storage, ZezoProviderConfig};
use crate::providers::{Provider, Status, WindsSpec, Winds, Wind};
use crate::error::{Error, Result};
use crate::stamp::{Durations, ForecastTime, ForecastTimeSpec, RefTime, RefTimeSpec, Stamp};

pub struct Zezo {
    pub(crate) status: Winds,
    pngs: Storage,
}

impl Zezo {
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

    pub fn new(pngs_dir: String) -> Result<Self> {
        Self::create_dir(&(&pngs_dir).into());

        Ok(Self {
            status: Arc::new(RwLock::new(Status {
                provider: "zezo".to_string(),
                provider_name: "Zezo".to_string(),
                current_ref_time: Self::current_ref_time(),
                last: None,
                progress: 0,
                forecasts: Default::default()
            })),
            pngs: Storage::Local { dir: pngs_dir },
        })

    }

    pub(crate) fn from_config(config: &ZezoProviderConfig) -> Result<Self> {
        match &config.pngs {
            Storage::Local{dir} => Self::create_dir(&dir.into()),
            _ => {}
        }

        Ok(Self {
            status: Arc::new(RwLock::new(Status {
                provider: "zezo".to_string(),
                provider_name: "Zezo".to_string(),
                current_ref_time: Self::current_ref_time(),
                last: None,
                progress: 0,
                forecasts: Default::default()
            })),
            pngs: config.pngs.clone(),
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

            if !self.pngs.exists(stamp.file_name()).await? {

                match self.download_png(&stamp).await {
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
                        error!("Error downloading png `{}` : {:?}", stamp, e);
                        break;
                    }
                }
            }

            h += self.step();
            first = false;
        }

        Ok(something_new)
    }

    async fn download_png(&self, stamp: &Stamp) -> Result<()> {

        let url = format!("http://fr.zezo.org/windp/{}_{:03}_{}.png", stamp.forecast_time.format("%Y%m%d"), stamp.forecast_time.hour(), stamp.ref_time.hour());

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        let req = client.get(url).build()?;

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
                error!("Error downloading png file {} : {}", stamp, e);
                Err(Error::Error())
            }
        }
    }

    fn vr_speed(d: u8) -> f64 {
        if d > 127 {
            let d = 256.0 - d as f64;
            -(d * d) * (3600.0 / 230400.0) / 1.852
        } else {
            let d = d as f64;
            (d * d) * (3600.0 / 230400.0) / 1.852
        }
    }
}

#[async_trait]
impl Provider for Zezo {

    fn id(&self) -> String {
        String::from("zezo")
    }

    fn jsons_storage(&self) -> Storage {
        self.pngs.clone()
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

    async fn on_file_downloaded(&self, file: PathBuf, stamp: &Stamp) -> Result<()> {
        self.pngs.save(&file, stamp.file_name()).await?;

        std::fs::remove_file(&file).unwrap_or_default();

        Ok(())
    }

    async fn load_stamp(&self, stamp: &Stamp) -> Result<Wind> {

        let img = ImageReader::new(self.pngs.open(stamp.file_name()).await?).with_guessed_format()?.decode()?;

        let mut u = Vec::with_capacity(180);
        let mut v = Vec::with_capacity(180);

        for y in 0..180 {
            //let y = 179 - y;

            let mut raw_u = Vec::with_capacity(360);
            let mut raw_v = Vec::with_capacity(360);

            for x in 0..360 {
                //let lon = x - 180;

                let pixel = img.get_pixel(x, y);
                raw_u.push(Self::vr_speed(pixel.0[0]));
                raw_v.push(Self::vr_speed(pixel.0[1]));
            }
            raw_u.push(raw_u[0]);
            raw_v.push(raw_v[0]);
            u.push(raw_u.into_boxed_slice());
            v.push(raw_v.into_boxed_slice());
        }

        Ok(Wind {
            lat0: -90.0,
            lon0: -180.0,
            delta_lat: 1.0,
            delta_lon: 1.0,
            n_lat: 180,
            n_lon: 361,
            u: u.into_boxed_slice(),
            v: v.into_boxed_slice()
        })
    }
}
