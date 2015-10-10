#![feature(custom_derive, plugin,libc,box_patterns,custom_attribute,box_syntax)]
#![plugin(serde_macros)]

extern crate hyper;
extern crate chrono;
extern crate serde_json;
extern crate serde;
extern crate libc;
extern crate redis;
extern crate url;
extern crate itertools;

use std::io::Write;
use std::io::stderr;
use itertools::Itertools;
use redis::Commands;
use redis::Connection;
use url::Url;
use std::error::Error;
use std::str;
use std::io::Read;
use hyper::Client;
use std::ffi::CStr;
use std::ffi::CString;
use chrono::*;
use std::collections::BTreeMap;
use serde_json::to_string;
use serde_json::from_str;
use serde_json::{Value};
use std::fmt::Display;
use std::fmt::Formatter;
use serde::{Deserialize, Deserializer, Serializer, Serialize};

const RESULT_LENGTH: usize = 30;
const PRE_TRIM_NUM: usize = 30;

macro_rules! timeit {
    ($e: expr, $f: expr, $t: expr) => {
        {
            let t = UTC::now();
            let result = $e;
            if $t { let _ = writeln!(stderr(), "keen native: {} :{}", $f, UTC::now() - t);}
            result
        }
    }
}

#[derive(Debug,Deserialize,Serialize)]
struct KeenResult {
    result: Vec<Day>
}

#[derive(Debug,Deserialize,Serialize)]
struct Day {
    value: Vec<Page>,
    timeframe: Timeframe
}

#[derive(Debug)]
struct Page {
    result: u64,
    page_id: usize,
    group: Group
}

#[derive(Debug, Clone)]
enum Group {
    Referrer(String), // normalized_referrer
    Country(String),   // ip_geo_info.country
    None
}

impl Deserialize for Page {
    fn deserialize<D>(deserializer: &mut D) -> Result<Page, D::Error> where D: Deserializer {
        use serde::de::Error;
        let mut object: BTreeMap<String, Value> = try!(Deserialize::deserialize(deserializer));
        let result = try!(object.remove("result").and_then(|v| v.as_u64()).ok_or(D::Error::missing_field("no such field: result")));
        let page_id = try!(object.remove("pageId").and_then(|v| v.as_u64()).ok_or(D::Error::missing_field("no such field: pageId")));
        let referrer = object.remove("normalized_referrer")
            .and_then(|f| f.as_string().map(|f| Group::Referrer(f.into())));
        let country = object.remove("ip_geo_info.country")
            .and_then(|f| f.as_string().map(|f| Group::Country(f.into())));
        let group = referrer.or(country).or(Some(Group::None)).unwrap();
        Ok(Page {
            result: result,
            page_id: page_id as usize,
            group: group
        })
    }
}

impl Serialize for Page {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut bt = BTreeMap::new();
        bt.insert("result".to_owned(), Value::U64(self.result));
        bt.insert("pageId".to_owned(), Value::U64(self.page_id as u64));
        if self.group_name() != "" {
            bt.insert(self.group_name().into(), Value::String(self.group_value().into()));
        }
        bt.serialize(serializer)
    }
}

impl Page {
    fn group_name(&self) -> &'static str {
        match self.group {
            Group::Country(_) => "ip_geo_info.country",
            Group::Referrer(_) => "normalized_referrer",
            Group::None => ""
        }
    }
    fn group_value(&self) -> &str {
        match self.group {
            Group::Country(ref s) => &s,
            Group::Referrer(ref s) => &s,
            Group::None => ""
        }
    }
}

#[derive(Debug,Deserialize,Serialize)]
struct Timeframe {
    start: String,
    end: String
}

#[derive(Debug,Deserialize,Serialize)]
struct KeenError {
    message: String,
    error_code: String
}

impl Display for KeenError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}: {}", self.error_code, self.message))
    }
}

impl Error for KeenError {
    fn description(&self) -> &str {
        &self.message
    }
}

#[derive(Debug,Deserialize,Serialize)]
struct NativeError {
    error: String
}

impl Display for NativeError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}", self.error))
    }
}

impl Error for NativeError {
    fn description(&self) -> &str {
        &self.error
    }
}

impl NativeError {
    fn new<S:Into<String>>(s: S) -> NativeError {
        NativeError {
            error: s.into()
        }
    }
}

pub struct KeenOptions {
    url: String,
    page_id: usize,
    from_date: DateTime<UTC>,
    redis_conn: Option<String>,
    aggregate: bool,
    debug: bool
}

impl KeenOptions {
    pub fn new(url: &str, page_id: usize, time: DateTime<UTC>) -> KeenOptions {
        KeenOptions {
            url: url.to_owned(),
            page_id: page_id,
            from_date: time,
            redis_conn: None,
            aggregate: false,
            debug: false
        }
    }
    pub fn set_debug(&mut self, debug: bool) {
        self.debug = debug;
    }
    pub fn set_aggregate(&mut self, aggregate: bool) {
        self.aggregate = aggregate;
    }
    pub fn set_redis(&mut self, redis: &str) {
        self.redis_conn = Some(redis.into());
    }

    pub fn get_data(self) -> Result<String, Box<Error>> {
        let KeenOptions {
            url, page_id, from_date, redis_conn, aggregate, debug
        } = self;

        let parsed_url = try!(Url::parse(&url));
        let unique = url.contains("count_unique");

        let mut query: BTreeMap<String,String> = parsed_url.query_pairs().unwrap_or(vec![]).into_iter().collect();

        let expire = query.remove("max_age").or_else(|| {
            if debug { let _ = writeln!(stderr(), "keen native: no max_age specific, use 300 as default"); };
            None
        }).unwrap_or("300".to_owned()).parse().unwrap_or(300);

        let redis_conn = redis_conn.map(|r| {
            if debug { let _ = writeln!(stderr(), "keen native: redis conn: {}", r); }
            r
        });

        let conn = redis_conn
            .and_then(|conn| generate_redis_key(query,unique).ok().map(|key| (conn, key)))
            .and_then(|(conn, key)| {
                if debug { let _ = writeln!(stderr(), "keen native: redis key: {}", key); }
                redis::Client::open(&conn[..])
                    .and_then(|client| client.get_connection())
                    .map_err(|e| writeln!(stderr(), "keen native: redis error: {}", e)).ok()
                    .map(|conn| if test_key_of_redis(&conn, &key) { (conn,key,true) } else { (conn,key,false) })
            });

        let data = try! {
            match conn {
                Some((conn, key, true)) => {
                    timeit!(get_data_from_redis(&conn, &key), "get data from redis", debug)
                }
                Some((conn, key, false)) => {
                    timeit!{
                        timeit!(get_keen_raw_data(&url), "get data from keen", debug).and_then(|data| {
                            if data.starts_with(r#"{"message":"#) {
                                let err: KeenError = try!(from_str(&data));
                                try!(Err(err));
                            }

                            let iter = day_iter(&data);
                            let ret = KeenResult{
                                result: pre_trim(iter).collect()
                            };

                            let s = to_string(&ret).unwrap();
                            let _ = timeit!(set_data_to_redis(&conn, &key, &s, expire), "set data to redis", debug);
                            Ok(s)
                        }), "get && set data to redis", debug
                    }
                }
                _ => timeit!(get_keen_raw_data(&url).and_then(|data| {
                    if data.starts_with(r#"{"message":"#) {
                        let err: KeenError = try!(from_str(&data));
                        try!(Err(err));
                    }
                    Ok(data)
                }), "get keen raw data", debug)
            }
        };

        let mut field = false;

        let days = timeit! {
            day_iter(&data).map(|mut day| {
                day.value = day.value.into_iter().filter(|page| {
                    if page.group_name() != "" {
                        field = true
                    }
                    page.page_id == page_id
                }).collect();
                day
            }).filter_map(|day| {
                if day.timeframe.start.parse::<DateTime<UTC>>()
                    .map(|datetime| datetime >= from_date).unwrap_or(false) {
                        Some(day)
                    } else {
                        None
                    }
            }).collect(), "filter", debug
        };

        let result = try!(timeit!(transform(days, aggregate, field), "transform", debug));

        Ok(result)
    }
}

fn day_iter<'a>(data: &'a str) -> Box<Iterator<Item=Day> + 'a> {
    let to_split = data
        .trim_left_matches(r#"{"result": ["#) // this for json from keen
        .trim_left_matches(r#"{"result":["#) // this for json serialized by us
        .trim_right_matches(r#"]}"#); // this is same with both keen and us

    let elems = split_json_to_elem(to_split);
    box elems.into_iter()
        .filter_map(|daystr| from_str::<Day>(daystr).map_err(|e| writeln!(stderr(), "deserialize fail: {}, {}", e, daystr)).ok()) as Box<Iterator<Item=Day>>
}

fn pre_trim<'a,I>(days: I) -> Box<Iterator<Item=Day> + 'a> where I: std::iter::Iterator<Item=Day>, I: 'a {
    box days.map(|mut day| {
        day.value = day.value.into_iter().filter(|p| p.result != 0)
            .group_by(|p| p.page_id)
            .flat_map(|(pid, mut records)| {
                if records.len() > 1 {
                    records.sort_by(|a,b| b.result.cmp(&a.result));
                    let (less, more): (Vec<_>, Vec<_>) = records.into_iter().enumerate().partition(|&(idx, _)| idx < PRE_TRIM_NUM);

                    let result = more.into_iter().fold(0, |acc, e| acc + e.1.result);

                    let others_group = match less[0].1.group {
                        Group::Country(_)   => Group::Country("others".into()),
                        Group::Referrer(_)  => Group::Referrer("others".into()),
                        Group::None         => {
                            let _ = writeln!(stderr(), "unreachable branch: error! {:?}", less[0]);
                            Group::None
                        }
                    };

                    let mut less: Vec<_> = less.into_iter().map(|e| e.1).collect();
                    less.push(Page {
                        result: result,
                        page_id: pid,
                        group: others_group
                    });
                    less.into_iter()
                } else {
                    records.into_iter()
                }
            }).collect();
        day
    }) as Box<Iterator<Item=Day>>
}

fn transform(data: Vec<Day>, aggregate: bool, field: bool) -> Result<String, Box<Error>> {
    let arr_of_day = data;

    if aggregate {
        let mut group = None;
        let mut kv = BTreeMap::new();
        for day in arr_of_day.iter() {
            for page in day.value.iter() {
                group = group.or(Some(page.group_name()));
                *kv.entry(page.group_value()).or_insert(0) += page.result;
            }
        };

        if kv.len() == 0 {
            if field {
                Ok(try!(to_string(&Vec::<()>::new())))
            } else {
                Ok(try!(to_string(&0)))
            }
        } else if kv.len() == 1 && kv.contains_key("") { // this means sum up'em all, no need to group by any group
            Ok(try!(to_string(&kv.remove("").unwrap_or(0))))
        } else {
            let group = if let Some(group) = group {
                group
            } else {
                return try!(Err(NativeError::new("cannot find group name")));
            };

            let mut arr: Vec<BTreeMap<&str, Value>> = kv.into_iter().map(|(name, value)| {
                vec![("result", Value::U64(value)), (group, Value::String(name.into()))]
                    .into_iter().collect::<BTreeMap<_,_>>()
            }).collect();

            // there's too many rows sometimes, for popular sites. reduce it when needed
            arr.sort_by(|a, b| {
                let a = a.get("result").map(|v| v.as_u64().unwrap_or(0)).unwrap_or(0);
                let b = b.get("result").map(|v| v.as_u64().unwrap_or(0)).unwrap_or(0);
                b.cmp(&a)
            });

            let l = if arr.len() > RESULT_LENGTH { RESULT_LENGTH } else { arr.len() };
            let arr = &arr[0..l];
            Ok(try!(to_string(&arr)))
        }
    } else {
        let arr: Vec<_> = arr_of_day.into_iter().map(|day| {
            let mut bt = BTreeMap::new();
            let time_bmap = vec![("start".to_owned(), Value::String(day.timeframe.start)), ("end".to_owned(), Value::String(day.timeframe.end))].into_iter().collect();
            bt.insert("timeframe", Value::Object(time_bmap));

            if day.value.len() == 0 {
                bt.insert("value", Value::U64(0));
            } else if day.value.len() == 1 {  // if value only contains 1 elem, flatten it
                bt.insert("value", Value::U64(day.value[0].result));
            } else {
                bt.insert("value", Value::Array(day.value.into_iter().map(|page| {
                    let mut bt = BTreeMap::new();
                    bt.insert("result".into(), Value::U64(page.result));
                    bt.insert(page.group_name().into(), Value::String(page.group_value().into()));
                    Value::Object(bt)
                }).collect()));
            }
            bt
        }).collect();
        Ok(try!(to_string(&arr)))
    }
}

fn test_key_of_redis(conn: &Connection, key: &str) -> bool {
    conn.exists(key).unwrap_or(false)
}

fn generate_redis_key(mut bt: BTreeMap<String,String>, unique: bool) -> Result<String,Box<Error>> {
    let unique = if unique { "count_unique" } else { "count" };
    let target_property: String = try!(bt.remove("target_property").ok_or(NativeError::new("no such query in url: target_property".to_owned())));
    let group_by: String = try!(bt.remove("group_by").ok_or(NativeError::new("no such query in url: group_by".to_owned())));
    let interval: String = try!(bt.remove("interval").ok_or(NativeError::new("no such query in url: interval".to_owned())));
    let timeframe: String = try!(bt.remove("timeframe").ok_or(NativeError::new("no such query in url: timeframe".to_owned())));
    let filters: String = try!(bt.remove("filters").ok_or(NativeError::new("no such query in url: filters".to_owned())));

    if interval == "daily"{
        Ok(format!("{}.{}.{}.{}.{}.{}", unique, target_property, group_by, filters, interval, timeframe))
    } else {
        Ok(format!("{}.{}.{}.{}.{}", unique, target_property, group_by, filters, interval))
    }
}

fn split_json_to_elem<'a>(json: &'a str) -> Vec<&'a str> {
    let mut count = 0;
    let mut new = true;
    let mut vec = vec![];
    let jsonb = json.as_bytes();
    let mut begin = 0;
    for i in 0 .. json.len() {
        match jsonb[i] as char {
            '{' => {
                if count == 0 {
                    new = true;
                    begin = i;
                }
                count += 1;
            }
            '}' => count -= 1,
            _ => {}
        }
        if count == 0 && new {
            vec.push(&json[begin..i+1]);
            new = false;
        }
    }
    vec
}

fn get_data_from_redis<'a>(conn: &'a Connection, key: &'a str) -> Result<String, Box<Error>> {
    let result = try!(conn.get(key));
    Ok(result)
}

fn set_data_to_redis<'a>(conn: &'a Connection, key: &'a str, value: &str, timeout: usize) -> Result<(), Box<Error>> {
    let _: () = try!(conn.set(key, value));
    let _: () = try!(conn.expire(key, timeout));
    Ok(())
}

fn get_keen_raw_data(url: &str) -> Result<String, Box<Error>> {
    let mut resp = try!(Client::new().get(url).send());
    let mut s = Vec::with_capacity(30 * 1024 * 1024);
    let _ = resp.read_to_end(&mut s);
    let s = unsafe {String::from_utf8_unchecked(s)};
    Ok(s)
}

#[no_mangle]
pub extern "C" fn new_options(url: *const libc::c_char, page_id: i32, after_day: *const libc::c_char) -> *const KeenOptions {
    let url = unsafe {CStr::from_ptr(url)};
    let url = url.to_bytes();
    let url = unsafe {str::from_utf8_unchecked(url)};
    let after_day = unsafe {CStr::from_ptr(after_day)};
    let after_day = after_day.to_bytes();
    let after_day = unsafe{str::from_utf8_unchecked(after_day)};

    let time: DateTime<UTC> = match after_day.parse() {
        Ok(time) => time,
        Err(e) => return CString::new(to_string(&NativeError::new(format!("{:?}", e))).unwrap()).unwrap().into_raw() as *mut _,
    };

    let ret = Box::new(KeenOptions::new(url, page_id as usize, time));
    Box::into_raw(ret)
}

#[no_mangle]
pub extern "C" fn set_redis(options: *mut KeenOptions, conn: *const libc::c_char) {
    let conn = unsafe {CStr::from_ptr(conn)};
    let conn = conn.to_bytes();
    let conn = unsafe {str::from_utf8_unchecked(conn)};

    let mut options = unsafe {Box::from_raw(options)};
    options.redis_conn = Some(conn.to_owned());
    let _ = Box::into_raw(options);
}

#[no_mangle]
pub extern "C" fn set_debug(options: *mut KeenOptions, debug: bool) {
    let mut options = unsafe {Box::from_raw(options)};
    options.debug = debug;
    let _ = Box::into_raw(options);
}

#[no_mangle]
pub extern "C" fn set_aggregate(options: *mut KeenOptions) {
    let mut options = unsafe {Box::from_raw(options)};
    options.aggregate = true;
    let _ = Box::into_raw(options);
}

#[no_mangle]
pub extern "C" fn dealloc_str(s: *mut libc::c_char) {
    unsafe {CString::from_raw(s)};
}

#[no_mangle]
pub extern "C" fn get_data(options: *mut KeenOptions) -> *const libc::c_char {
    let option = unsafe { Box::from_raw(options) };
    match option.get_data() {
        Ok(result) => CString::new(format!(r#"{{"result": {}}}"#, result)).unwrap().into_raw(),
        Err(e) => CString::new(to_string(&NativeError::new(format!("{:?}", e))).unwrap()).unwrap().into_raw()
    }
}
