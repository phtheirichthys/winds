use crate::stamp::StampError;
use crate::grib;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error")]
    Error(),

    #[error("StampError: {0}")]
    StampError(#[from] StampError),

    #[error("StampNotFoundError")]
    StampNotFoundError(),

    #[error("RoundingError: {0}")]
    RoundingError(#[from] chrono::RoundingError),

    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Infallible: {0}")]
    Infallible(#[from] std::convert::Infallible),

    #[error("ReqwestError: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("ExitStatusError: {0}")]
    ExitStatusError(#[from] std::process::ExitStatusError),

    #[error("GribError: {0}")]
    GribError(#[from] grib::GribError),

    #[error("ImageError: {0}")]
    ImageError(#[from] image::ImageError),

    #[error("AnyhowError: {0}")]
    AnyhowError(#[from] anyhow::Error),

}