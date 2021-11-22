use rocket::http::Status;
use rocket::{Rocket, Build};

mod v1;

pub(crate) fn build() -> Rocket<Build> {
  rocket::build().mount("/healthz/-", routes![ready])
}

#[get("/ready")]
async fn ready() -> Status {
  Status::Ok
}