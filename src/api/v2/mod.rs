mod model;

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Deref;
use chrono::NaiveDateTime;
use rocket::{response, Request, Response, Route, State};
use rocket::http::{hyper, Header, Status};
use rocket::response::Responder;
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

struct GribData {
    data: Vec<u8>,
}

#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for GribData {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        Response::build()
            .raw_header("Cache-Control", "max-age=43200")
            .sized_body(self.data.len(), Cursor::new(self.data))
            .ok()
    }
}

#[get("/winds/<provider>/<ref_time>/<forecast_time>")]
async fn get_ref(winds: &State<HashMap<String, Winds>>, provider: String, ref_time: String, forecast_time: String) -> Result<GribData, Status> {
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
            let content = match std::fs::read(&format!("data/noaa/{}", stamp.file_name())) {
                Ok(content) => content,
                Err(e) => {
                    debug!("error getting file : {}", e);
                    return Err(Status::NotFound)
                }
            };
            Ok(GribData {data: content})
        },
        None => Err(Status::NotFound)
    }
}
