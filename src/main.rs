#![feature(hash_drain_filter, async_closure)]
#![feature(exit_status_error)]

use std::collections::HashMap;
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
        std::env::set_var("RUST_LOG", "error,winds=info");
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

    match api::build().manage(winds).launch().await {
        Ok(_) => (),
        Err(e) => {
            error!("Error launching server : {:?}", e);
        }
    }
}
