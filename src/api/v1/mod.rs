mod model;

use std::collections::HashMap;
use std::ops::Deref;
use rocket::{Route, State};
use rocket::http::Status;
use rocket::serde::json::Json;
use crate::api::v1::model::Forecasts;
use crate::providers::Winds;

pub(crate) fn routes() -> Vec<Route> {
    routes![get]
}

#[get("/winds?<provider>")]
async fn get(winds: &State<HashMap<String, Winds>>, provider: String) -> Result<Json<Forecasts>, Status> {
    match winds.get(&provider) {
        Some(winds) => {
            let forecasts: Forecasts = winds.read().await.deref().into();
            Ok(Json(forecasts))
        },
        None => Err(Status::NotFound)
    }
}
