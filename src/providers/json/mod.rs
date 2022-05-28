use std::fs::File;
use std::io::BufReader;
use anyhow::Result;
use serde::Deserialize;
use std::path::PathBuf;
use crate::providers::Wind;
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

pub async fn load_all(stamps: &Vec<Stamp>) -> Result<Vec<Wind>> {

    let mut res = Vec::new();
    for stamp in stamps {
        res.push(load(stamp.file_name().into()).await?);
    }

    Ok(res)
}

pub async fn load(json_filename: PathBuf) -> Result<Wind> {

    let f = File::open(&json_filename)?;
    let f = BufReader::new(f);

    let messages: Vec<Message> = serde_json::from_reader(f)?;

    Ok(messages.try_into()?)
}
