extern crate hyper;
extern crate chrono;
extern crate serde_json;
extern crate serde;
extern crate libc;
extern crate redis;
extern crate keen;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

#[macro_use]
mod client;
mod errors;
mod protocol;
mod ffi;

#[no_mangle]
pub use ffi::*;
pub use client::{KeenCacheClient, KeenCacheQuery, KeenCacheResult};
pub use protocol::{Accumulate, Days, KeenError, KeenResult, Range, Select, StringOrI64};
pub use keen::{Filter, Interval, KeenClient, KeenQuery, Metric, TimeFrame};