use rocket::http::Status;
use rocket::{Rocket, Build};

mod v2;

pub(crate) fn build() -> Rocket<Build> {
  rocket::build()
      .mount("/healthz/-", routes![ready])
      .mount("/winds/api/v2/", v2::routes())
}

#[get("/ready")]
async fn ready() -> Status {
  Status::Ok
}