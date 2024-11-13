mod model;

use std::collections::HashMap;
use std::ops::Deref;
use chrono::NaiveDateTime;
use rocket::{Route, State};
use rocket::http::Status;
use rocket::serde::json::Json;
use crate::api::v2::model::Forecasts;
use crate::providers::Winds;
use crate::stamp::Stamp;

pub(crate) fn routes() -> Vec<Route> {
    routes![get, get_ref]
}

#[get("/winds/<provider>")]
async fn get(winds: &State<HashMap<String, Winds>>, provider: String) -> Result<Json<Forecasts>, Status> {
    match winds.get(&provider) {
        Some(winds) => {
            let forecasts: Forecasts = winds.read().await.deref().into();
            Ok(Json(forecasts))
        },
        None => Err(Status::NotFound)
    }
}

#[get("/winds/<provider>/<ref_time>/<forecast_time>")]
async fn get_ref(winds: &State<HashMap<String, Winds>>, provider: String, ref_time: String, forecast_time: String) -> Result<Vec<u8>, Status> {
    match winds.get(&provider) {
        Some(winds) => {
            let forecast_time = match NaiveDateTime::parse_from_str(&format!("{}00", forecast_time), "%Y%m%d%H%M") {
                Ok(forecast_time) => forecast_time.and_utc(),
                Err(e) => {
                    debug!("error forecast_time {forecast_time} : {}", e);
                    return Err(Status::NotFound)
                }
            };
            let ref_time = match NaiveDateTime::parse_from_str(&format!("{}00", ref_time), "%Y%m%d%H%M") {
                Ok(ref_time) => ref_time.and_utc(),
                Err(e) => {
                    debug!("error ref_time {ref_time} : {}", e);
                    return Err(Status::NotFound)
                }
            };

            let stamp = Stamp { forecast_time, ref_time, wind: None };
            let content = match std::fs::read(&format!("data/noaa/jsons/{}", stamp.file_name())) {
                Ok(content) => content,
                Err(e) => {
                    debug!("error getting file : {}", e);
                    return Err(Status::NotFound)
                }
            };
            Ok(content)
        },
        None => Err(Status::NotFound)
    }
}
