use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use flate2::Compression;
use s3::Bucket;
use serde::{Serialize, Deserialize};

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Config {
  pub(crate) providers: Vec<ProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ProviderConfig {
  Noaa(NoaaProviderConfig),
  Meteofrance(MeteofranceProviderConfig)
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub(crate) struct NoaaProviderConfig {
  pub(crate) enabled: bool,
  pub(crate) init: Option<DateTime<Utc>>,
  pub(crate) gribs_dir: String,
  pub(crate) jsons: Vec<Storage>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub(crate) struct MeteofranceProviderConfig {
  pub(crate) enabled: bool,
  token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Storage {
  Local{
    dir: String
  },
  ObjectStorage {
    endpoint: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
  }
}

impl Display for Storage {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Storage::Local { dir } => {
        write!(f, "Local ({})", dir)
      }
      Storage::ObjectStorage { bucket, .. } => {
        write!(f, "ObjectStorage ({})", bucket)
      }
    }
  }
}

impl Storage {

  pub(crate) async fn save<P: AsRef<Path>>(&self, from: P, name: String) -> anyhow::Result<()> {

    match self {
      Storage::Local {dir} => {
        fs::copy(from, Path::new(&dir).join(&name))?;
      },
      Storage::ObjectStorage { endpoint, region, bucket , access_key, secret_key} => {
        let mut storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();

        storage.set_path_style();
        storage.add_header("content-encoding", "gzip");

        let file = File::open(from)?;

        let mut gz = flate2::bufread::GzEncoder::new(BufReader::new(file), Compression::best());
        let mut buffer = Vec::new();
        gz.read_to_end(&mut buffer)?;

        let (_, status_code) = storage.put_object_with_content_type(&name, buffer.as_slice(), "application/json").await?;

        if status_code != 200 {
          return Err(anyhow!("Error saving file to s3 bucket : {}", status_code));
        }
      }
    }

    debug!("File `{}` saved on storage {}", &name, self);

    Ok(())
  }

  pub(crate) async fn remove(&self, name: String) -> anyhow::Result<()> {

    match self {
      Storage::Local { dir } => {
        fs::remove_file(Path::new(dir).join(name))?;
      }
      Storage::ObjectStorage { endpoint, region, bucket, access_key, secret_key } => {
        let storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();

        let (_, status_code) = storage.delete_object(name).await?;

        if status_code != 204 {
          return Err(anyhow!("Error deleting file from s3 bucket : {}", status_code));
        }
      }
    }

    Ok(())
  }
}