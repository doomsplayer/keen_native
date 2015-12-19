extern crate libc;
extern crate keenio_booster;
use keenio_booster::KeenCacheClient;
use libc::{c_char, c_int, c_void};
use std::env;
use std::mem;
use std::ffi;

#[link(name = "keenio_booster")]
extern "C" {
    fn new(key: *const c_char, project: *const c_char) -> *mut c_void;
    fn set_redis(c: *mut c_void, url: *const c_char) -> bool;
    fn query(c: *mut c_void,
             metric_type: c_int,
             metric_target: *mut c_char,
             collection: *const c_char,
             start: *const c_char,
             end: *const c_char)
             -> *mut c_void;
    fn group_by(q: *mut c_void, group: *const c_char) -> bool;
    fn filter(q: *mut c_void,
              filter_type: c_int,
              filter_a: *const c_char,
              filter_b: *const c_char)
              -> bool;
    fn data(q: *mut c_void, tp: c_int) -> *mut c_void;
    fn enable_log();
    fn result_data(r: *mut c_void) -> c_char;
    fn delete_result(r: *mut c_void);
    fn to_redis(r: *mut c_void, key: *const c_char, expire: c_int) -> bool;
}

fn main() {
    unsafe { enable_log() };

    let key = env::var("KEEN_READ_KEY").unwrap();
    let project = env::var("KEEN_PROJECT_ID").unwrap();
    let redis = env::var("REDISCLOUD_URL").unwrap();
    let client = unsafe { new(ffi::CString::new(key).unwrap().as_ptr() , ffi::CString::new(project).unwrap().as_ptr()) };
    unsafe { set_redis(client, ffi::CString::new(redis).unwrap().as_ptr()) };
    let mut q = unsafe { query(
        client,
        keenio_booster::COUNT,
        0 as *mut _,
        ffi::CString::new("strikingly_pageviews").unwrap().as_ptr(),
        ffi::CString::new("2015-12-18T20:32:36+08:00").unwrap().as_ptr(),
        ffi::CString::new("2015-12-19T20:32:36+08:00").unwrap().as_ptr())
    };
    unsafe { filter(q, keenio_booster::GT, ffi::CString::new("pageId").unwrap().as_ptr(), ffi::CString::new("1").unwrap().as_ptr()) };
    unsafe { filter(q, keenio_booster::LT, ffi::CString::new("pageId").unwrap().as_ptr(), ffi::CString::new("10000").unwrap().as_ptr()) };
    unsafe { group_by(q, ffi::CString::new("pageId").unwrap().as_ptr()) };
    let mut d = unsafe { data(q, keenio_booster::ITEMS) };
    unsafe { to_redis(d, ffi::CString::new("key123").unwrap().as_ptr(), 123 as c_int) };
    unsafe { delete_result(d) };
    q = 0 as *mut _;
    d = 0 as *mut _;
    let _ = unsafe {Box::from_raw(client as *mut KeenCacheClient) };
}
