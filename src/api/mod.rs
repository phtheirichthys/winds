use rocket::http::Status;
use rocket::{Rocket, Build};

mod v1;

pub(crate) fn build() -> Rocket<Build> {
  rocket::build()
      .mount("/healthz/-", routes![ready])
      .mount("/winds/api/v1/", v1::routes())
}

#[get("/ready")]
async fn ready() -> Status {
  Status::Ok
}