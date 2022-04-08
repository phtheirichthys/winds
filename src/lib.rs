#![feature(btree_drain_filter, async_closure)]
#![feature(exit_status_error)]
#![feature(map_first_last)]
#![feature(is_some_with)]

extern crate log;
#[macro_use]
extern crate rocket;

mod api;
pub mod config;
pub mod providers;
mod error;
mod stamp;
mod grib;
