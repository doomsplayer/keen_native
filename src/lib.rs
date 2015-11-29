extern crate hyper;
extern crate chrono;
extern crate serde_json;
extern crate serde;
extern crate libc;
extern crate redis;
extern crate keen;
extern crate env_logger;
#[macro_use] extern crate wrapped_enum;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;

#[macro_use] mod client;
mod error;
mod protocol;
mod cache;
mod ffi;

#[no_mangle]
pub use ffi::*;
pub use client::*;
pub use protocol::*;


pub fn logger() {
    std::env::set_var("RUST_LOG", "keenio_booster");
    std::env::set_var("RUST_BACKTRACE", "1");
    let _ = env_logger::init();
}
