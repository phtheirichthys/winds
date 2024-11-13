#![feature(async_closure)]
#![feature(exit_status_error)]

use std::collections::HashMap;
use rocket::http::Method;
use rocket_cors::{AllowedHeaders, AllowedOrigins};
use structopt::StructOpt;

extern crate log;
#[macro_use]
extern crate rocket;

mod api;
mod config;
mod providers;
mod error;
mod stamp;

#[derive(Debug, StructOpt)]
struct Cli {
    /// config file
    #[structopt(long = "config", short = "c", default_value = "config.yaml")]
    config_file: String,
}

#[rocket::main]
async fn main() -> () {
    std::env::var("RUST_LOG").map_err(|_| {
        std::env::set_var("RUST_LOG", "error,winds=debug");
    }).unwrap_or_default();
    env_logger::init();

    let args = Cli::from_args();

    let config: config::Config = confy::load_path(std::path::Path::new(&args.config_file)).unwrap();

    let mut winds = HashMap::new();
    for provider_config in config.providers {
        match providers::start_provider(&provider_config).await {
            Ok(Some(status)) => {
                let name = status.read().await.provider.clone();
                winds.insert(name, status);
            },
            Ok(None) => {},
            Err(e) => error!("Error starting provider `{:?}` : {:?}", provider_config, e)
        }
    }

    let cors = rocket_cors::CorsOptions {
        allowed_origins: AllowedOrigins::All,
        allowed_methods: vec![Method::Get].into_iter().map(From::from).collect(),
        allowed_headers: AllowedHeaders::some(&["Authorization", "Accept"]),
        allow_credentials: true,
        ..Default::default()
    }
    .to_cors().unwrap();

    match api::build().manage(winds).attach(cors).launch().await {
        Ok(_) => (),
        Err(e) => {
            error!("Error launching server : {:?}", e);
        }
    }
}
