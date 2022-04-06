use std::fs::File;
use std::io::BufReader;
use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use crate::providers::{build_grid, Wind};
use crate::stamp::Stamp;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub header: Header,
    pub data: Box<[f64]>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    pub discipline: u8,
    pub parameter_category: u8,
    pub parameter_number: u8,
    pub surface1_type: u8,
    pub surface1_value: f64,
    pub nx: usize,
    pub ny: usize,
    pub la1: f64,
    pub lo1: f64,
    pub dx: f64,
    pub dy: f64,
}

pub async fn load_all(jsons_dir: PathBuf, stamps: &Vec<Stamp>) -> Result<Vec<Wind>> {

    let mut res = Vec::new();
    for stamp in stamps {
        res.push(load(jsons_dir.join(stamp.file_name())).await?);
    }

    Ok(res)
}

pub async fn load(json_filename: PathBuf) -> Result<Wind> {

    let f = File::open(&json_filename)?;
    let f = BufReader::new(f);

    let messages: Vec<Message> = serde_json::from_reader(f)?;

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
                        debug!("U ok")
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

    wind.ok_or(anyhow!("Error loading wind from file {:?}", &json_filename))
}
