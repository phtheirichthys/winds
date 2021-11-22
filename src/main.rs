use structopt::StructOpt;

extern crate log;
#[macro_use] extern crate rocket;

mod api;
mod config;
mod wind;

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

  match api::build().launch().await {
    Ok(_) => (),
    Err(e) => {
      error!("Error launching server : {:?}", e);
    }
  }
}
