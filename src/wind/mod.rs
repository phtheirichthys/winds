use chrono::{DateTime, Utc};
use std::collections::HashMap;

type Providers = HashMap<String, dyn Provider>;

trait Provider {
  fn load(&self) {
    
  }
  
  fn start(&self);
  
  fn last_ref_time(&self) -> DateTime<Utc>;
  
  fn last_forecast_time(&self) -> DateTime<Utc>;
  
  fn next_update_time(&self) -> DateTime<Utc>;
  
  fn progress(&self) -> u8;
  
  fn forcasts(&self) -> HashMap<String, Vec<String>>;
  
  fn next(&self, time: DateTime<Utc>) -> bool;
  
  fn clean(&self);
}