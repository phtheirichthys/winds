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
use crate::providers::json::Message;
use crate::stamp::Stamp;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
  pub providers: Vec<ProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProviderConfig {
  Noaa(NoaaProviderConfig),
  Meteofrance(MeteofranceProviderConfig),
  Zezo(ZezoProviderConfig),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NoaaProviderConfig {
  pub enabled: bool,
  pub init: Option<DateTime<Utc>>,
  pub jsons: Storage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZezoProviderConfig {
  pub enabled: bool,
  pub init: Option<DateTime<Utc>>,
  pub pngs: Storage,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct MeteofranceProviderConfig {
  pub(crate) enabled: bool,
  token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Storage {
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
        storage.add_header("cache-control", "public, max-age=604800, immutable");

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

  pub(crate) async fn exists(&self, name: String) -> anyhow::Result<bool> {
    match self {
      Storage::Local { dir } => {
        Ok(Path::new(dir).join(name).exists())
      }
      Storage::ObjectStorage { endpoint, region, bucket, access_key, secret_key } => {
        let storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();

        let list_result = storage.list(String::from("/"), Some(String::from("/"))).await?;
        Ok(list_result.iter().find(|o| o.name == name).is_some())
      }
    }
  }

  pub(crate) fn exists_blocking(&self, name: String) -> anyhow::Result<bool> {
    match self {
      Storage::Local { dir } => {
        Ok(Path::new(dir).join(name).exists())
      }
      Storage::ObjectStorage { endpoint, region, bucket, access_key, secret_key } => {
        let storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();

        let list_result = storage.list_blocking(String::from("/"), Some(String::from("/")))?;
        Ok(list_result.iter().find(|o| o.name == name).is_some())
      }
    }
  }

  pub(crate) async fn list(&self) -> anyhow::Result<Vec<Stamp>> {
    match self {
      Storage::Local { dir } => {
        let mut stamps: Vec<Stamp> = Vec::new();

        // Walk throw json files
        let paths = fs::read_dir(dir)?;
        for entry in paths {
          if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
              if metadata.is_file() {
                stamps.push((&entry.path()).try_into()?);
              }
            }
          }
        }

        Ok(stamps)
      }
      Storage::ObjectStorage { endpoint, region, bucket, access_key, secret_key } => {
        let storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();

        let list_result = storage.list(String::from("/"), Some(String::from("/"))).await?;
        Ok(list_result.iter().filter_map(|o| {
          match Stamp::try_from(&o.name) {
            Ok(stamp) => Some(stamp),
            Err(_) => None,
          }
        }).collect())
      }
    }
  }

  pub(crate) async fn get(&self, name: String) -> anyhow::Result<Vec<Message>> {
    match self {
      Storage::Local { dir } => {
        let f = File::open(Path::new(dir).join(name))?;
        let f = BufReader::new(f);

        let messages: Vec<Message> = serde_json::from_reader(f)?;

        Ok(messages)
      }
      Storage::ObjectStorage { endpoint, region, bucket, access_key, secret_key } => {
        let storage = Bucket::new(&bucket, s3::Region::Custom{ region: region.clone(), endpoint: endpoint.clone() }, s3::creds::Credentials {
          access_key: Some(access_key.clone()),
          secret_key: Some(secret_key.clone()),
          security_token: None,
          session_token: None
        }).unwrap();


        let (buf, status_code) = storage.get_object(name).await?;

        let messages: Vec<Message> = serde_json::from_slice(buf.as_slice())?;

        if status_code != 204 {
          return Err(anyhow!("Error getting file from s3 bucket : {}", status_code));
        }

        Ok(messages)
      }
    }
  }

  pub(crate) async fn open(&self, name: String) -> anyhow::Result<BufReader<File>> {
    match self {
      Storage::Local { dir } => {
        let f = File::open(Path::new(dir).join(name))?;
        Ok(BufReader::new(f))
      }
      Storage::ObjectStorage { .. } => {
        todo!()
      }
    }
  }
}
