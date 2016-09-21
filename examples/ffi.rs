extern crate libc;
extern crate keenio_batch;
use keenio_batch::{ITEMS, COUNT, GT, LT};
use libc::{c_char, c_int, c_void};
use std::env;

macro_rules! cstr {
    ($var: expr) => {
        {
            use std::ffi::CStr;
            CStr::from_ptr($var).to_str().unwrap()
        }
    }
}

macro_rules! cstring {
    ($var: expr) => {
        {
            use std::ffi::CString;
            CString::new($var).unwrap().as_ptr()
        }
    }
}

#[link(name = "keenio_batch")]
extern "C" {
    fn new_client(key: *const c_char, project: *const c_char) -> *mut c_void;
    fn set_redis(c: *mut c_void, url: *const c_char) -> bool;
    fn new_query(c: *mut c_void,
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
    fn send(q: *mut c_void, tp: c_int) -> *mut c_void;
    fn to_string(r: *mut c_void) -> c_char;
    fn free_result(r: *mut c_void);
    fn free_query(r: *mut c_void);
    fn free_client(r: *mut c_void);
    fn free_string(r: *mut c_void);
    fn to_redis(r: *mut c_void, key: *const c_char, expire: c_int) -> bool;
    fn last_error() -> *const c_char;
}

fn main() {
    unsafe {
        let key = env::var("KEEN_READ_KEY").unwrap();
        let project = env::var("KEEN_PROJECT_ID").unwrap();
        let redis = env::var("REDISCLOUD_URL").unwrap();

        let client = new_client(cstring!(key), cstring!(project));
        set_redis(client, cstring!(redis));
        let query = new_query(client,
                              COUNT,
                              0 as *mut _,
                              cstring!("strikingly_pageviews"),
                              cstring!("2015-12-18T20:32:36+08:00"),
                              cstring!("2015-12-19T20:32:36+08:00"));
        filter(query, GT, cstring!("pageId"), cstring!("1"));
        filter(query, LT, cstring!("pageId"), cstring!("10000"));
        group_by(query, cstring!("pageId"));
        let data = send(query, ITEMS);
        if data == 0 as *mut _ {
            println!("{}", cstr!(last_error()));
            return;
        }
        to_redis(data, cstring!("key123"), 123 as c_int);

        free_result(data);
        free_client(client);
        free_query(query);
    }
}
