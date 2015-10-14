#![feature(custom_derive, plugin,libc,box_patterns,custom_attribute,box_syntax,const_fn)]
#![plugin(serde_macros)]

extern crate hyper;
extern crate chrono;
extern crate serde_json;
extern crate serde;
extern crate libc;
extern crate redis;
extern crate itertools;
extern crate keen;
extern crate regex;
#[macro_use] extern crate wrapped_enum;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;


use keen::*;
use chrono::*;

use std::*;
use std::io::{Write,Read,stderr};
use std::borrow::{Borrow, Cow};
use std::error::Error;
use std::ffi::{CStr, CString};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use regex::Regex;
use serde_json::{Value, to_string, from_str};
use serde::{Deserialize, Deserializer, Serializer, Serialize};

use redis::Commands;

const RESULT_LENGTH: usize = 30;
const PRE_TRIM_NUM: usize = 30;

macro_rules! timeit {
    ($e: expr, $f: expr, $t: expr) => {
        {
            let t = UTC::now();
            let result = $e;
            if $t { info!("keen native: {} :{}", $f, UTC::now() - t) }
            result
        }
    };
    ($e: expr, $f: expr) => {
        {
            let t = UTC::now();
            let result = $e;
            info!("{} :{}", $f, UTC::now() - t);
            result
        }
    };
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

wrapped_enum! {
    #[derive(Debug)]
    /// Error type of Keen Native library
    pub enum NativeError {
        /// native error for this library
        NativeError(Cow<'static, str>),
        /// json error
        JsonError(serde_json::error::Error),
        /// env error
        EnvError(env::VarError),
        /// redis error
        RedisError(redis::RedisError),
        /// hyper error
        HttpError(hyper::error::Error),
        /// chrono error
        TimeError(chrono::ParseError)
    }
}

impl Display for NativeError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}


impl Error for NativeError {
    fn description(&self) -> &str {
        use NativeError::*;

        match self {
            &NativeError(ref s) => s.borrow(),
            &JsonError(ref s) => s.description(),
            &EnvError(ref s) => s.description(),
            &HttpError(ref s) => s.description(),
            &RedisError(ref s) => s.description(),
            &TimeError(ref s) => s.description(),
        }
    }
}

pub type NativeResult<T> = Result<T, NativeError>;

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

fn transform(data: Vec<Day>, field: Option<&str>) -> NativeResult<String> {
    let arr_of_day = data;

    if let Some(group) = field {
        let mut kv = BTreeMap::new();
        for day in arr_of_day.iter() {
            for page in day.value.iter() {
                *kv.entry(page.group_value()).or_insert(0) += page.result;
            }
        };

        if kv.len() == 0 {
            Ok(try!(to_string(&Vec::<()>::new())))
        } else {
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

fn open_redis() -> NativeResult<redis::Connection> {
    let c = try!(env::var("REDISCLOUD_URL"));
    Ok(try!(redis::Client::open(&c[..]).and_then(|client| client.get_connection())))
}

fn generate_keen_client() -> NativeResult<KeenClient> {
    let keen_project = try!(env::var("KEEN_IO_PROJECT_ID"));
    let keen_read_key = try!(env::var("KEEN_READ_KEY"));

    let client = KeenClient::new(&keen_read_key, &keen_project);
    Ok(client)
}

fn generate_redis_key(metric: &str, target: Option<&str>, from: &str, to: &str, interval: Option<Interval>, bound: Option<(usize, usize)>) -> String {
    let mut s = "".to_owned();

    s.push_str(metric);

    target.map(|c| {
        s.push('.');
        s.push_str(c);
    });

    interval.map(|c| {
        s.push('.');
        s.push_str(&format!("{}", c));
    });

    bound.map(|c| {
        s.push('.');
        s.push_str(&format!("{}~{}", c.0, c.1));
    });

    s.push('.');
    s.push_str(from);
    s.push('~');
    s.push_str(to);

    s
}

lazy_static! {
    static ref SPIDER_FILTER: Filter = Filter::new("parsed_user_agent.device.family", Operator::Ne, "Spider");
}

const COLLECTION: &'static str = "strikingly_pageviews";

pub fn cache_total_page_view(from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<&'static str> {
    let client = try!(generate_keen_client());
    let redis = try!(open_redis());

    let timeout = if (to - from).num_days() > 1 {24 * 60 * 60} else {5 * 60};


    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();
    let metric = if unique {"count_unique"} else {"count"};

    let key = generate_redis_key(metric, None, &from_str, &to_str, None, None);

    let m = if unique {Metric::CountUnique("ip_address".into())} else {Metric::Count};
    let t = TimeFrame::Absolute(from, to);
    let c = COLLECTION.into();

    let mut q = client.query(m, c, t);
    q.add_filter(SPIDER_FILTER.clone());
    q.add_group("pageId");

    debug!("cache_total_page_view: url: {}", q.url());

    let mut resp = try!(timeit!(q.data(), "get data from keen io"));
    let mut v = vec![];
    let _ = resp.read_to_end(&mut v);
    let s = unsafe {String::from_utf8_unchecked(v)};


    let _: () = try!(redis.set(&key[..], s));
    let _: () = try!(redis.expire(&key[..], timeout));
    Ok("ok")
}

pub fn cache_page_view(pfrom: usize, pto: usize, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<&'static str> {
    let client = try!(generate_keen_client());
    let redis = try!(open_redis());

    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();

    let timeout = if (to - from).num_days() > 1 {24 * 60 * 60} else {5 * 60};
    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some("pageId"), &from_str, &to_str, None, Some((pfrom, pto)));

    let m = if unique {Metric::CountUnique("ip_address".into())} else {Metric::Count};
    let t = TimeFrame::Absolute(from, to);
    let c = COLLECTION.into();

    let mut q = client.query(m, c, t);
    q.interval(Interval::Daily);
    q.add_filter(SPIDER_FILTER.clone());
    q.add_group("pageId");
    q.add_filter(Filter::new("pageId", Operator::Gt, &format!("{}",pfrom)));
    q.add_filter(Filter::new("pageId", Operator::Lte, &format!("{}", pto)));

    debug!("cache_page_view: url: {}", q.url());

    let mut resp = try!(timeit!(q.data(), "get data from keen io"));
    let mut v = vec![];
    let _ = resp.read_to_end(&mut v);
    let s = unsafe {String::from_utf8_unchecked(v)};

    let _ = try!(redis.set(&key[..], s));
    let _ = try!(redis.expire(&key[..], timeout));
    Ok("ok")
}

pub fn cache_with_field(pfrom: usize, pto: usize, field: &str, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<&'static str> {
    let client = try!(generate_keen_client());
    let redis = try!(open_redis());

    let timeout = if (to - from).num_days() > 1 {24 * 60 * 60} else {5 * 60};

    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some(field), &from_str, &to_str, None, Some((pfrom, pto)));

    let m = if unique {Metric::CountUnique("ip_address".into())} else {Metric::Count};
    let t = TimeFrame::Absolute(from.clone(), to.clone());
    let c = COLLECTION.into();

    let mut q = client.query(m, c, t);
    q.interval(Interval::Daily);
    q.add_filter(SPIDER_FILTER.clone());
    q.add_group("pageId");
    q.add_group(field);
    q.add_filter(Filter::new("pageId", Operator::Gt, &format!("{}", pfrom)));
    q.add_filter(Filter::new("pageId", Operator::Lte, &format!("{}", pto)));

    debug!("cache_with_field: url: {}", q.url());

    let mut v = vec![];
    let _ = try!(timeit!(q.data(), "get data from keen io")).read_to_end(&mut v);
    let s = unsafe {String::from_utf8_unchecked(v)};

    let ret = KeenResult {
        result: pre_trim(day_iter(&s)).collect()
    };
    let s = to_string(&ret).unwrap();

    let _ = try!(redis.set(&key[..], s));
    let _ = try!(redis.expire(&key[..], timeout));
    Ok("ok")
}

pub fn get_total_page_view(page_id: usize, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<usize> {
    let redis = try!(open_redis());
    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, None, &from_str, &to_str, None, None);

    let s: String = try!(redis.get(&key[..]));
    let re = Regex::new(&format!(r#"\{{"pageId": {}, "result": (\d+)\}}"#, page_id)).unwrap();
    Ok(re.captures(&s).and_then(|g| {
        g.at(1).and_then(|d| d.parse().ok())
    }).unwrap_or(0))
}

pub fn get_with_field(page_id: usize, pfrom: usize, pto: usize, field: &str, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<String> {
    let redis = try!(open_redis());

    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some(field), &from_str, &to_str, None, Some((pfrom, pto)));

    let s: String = try!(redis.get(&key[..]));

    let days = timeit! {
        day_iter(&s).map(|mut day| {
            day.value = day.value.into_iter().filter(|page| {
                page.page_id == page_id
            }).collect();
            day
        }).collect(), "filter", true
    };

    timeit!(transform(days, Some(field)), "transform")
}

pub fn get_page_view(page_id: usize, pfrom: usize, pto: usize, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<String> {
    let redis = try!(open_redis());
    let from_str = from.date().and_hms(0,0,0).to_rfc3339();
    let to_str = to.date().and_hms(0,0,0).to_rfc3339();

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some("pageId"), &from_str, &to_str, None, Some((pfrom, pto)));

    let s: String = try!(redis.get(&key[..]));

    let days = timeit! {
        day_iter(&s).map(|mut day| {
            day.value = day.value.into_iter().filter(|page| {
                page.page_id == page_id
            }).collect();
            day
        }).collect(), "filter"
    };

    timeit!(transform(days, None), "transform")
}

// ffi bindings
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! returna {
    ($e: expr) => {
        match $e {
            Ok(o) => return make_result(o),
            Err(e) => return make_error(e)
        }
    }
}

macro_rules! returne {
    ($e: expr) => {
        match $e {
            Ok(o) => o,
            Err(e) => return make_error(e)
        }
    }
}

fn parse_day(d: *const libc::c_char) -> NativeResult<DateTime<UTC>> {
    let day = unsafe {CStr::from_ptr(d)};
    let day = day.to_bytes();
    let day = unsafe{str::from_utf8_unchecked(day)};
    Ok(try!(day.parse()))
}

fn make_error<E: Display>(s: E) -> *const libc::c_char {
    CString::new(format!(r#""error": {}"#, s)).unwrap().into_raw() as *const _
}

fn make_result<D: Display>(r: D) -> *const libc::c_char {
    CString::new(format!(r#""result": {}"#, r)).unwrap().into_raw() as *const _
}

#[no_mangle]
pub extern "C" fn cache_with_field_c(pfrom: libc::c_int, pto: libc::c_int, field: *const libc::c_char, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let field = unsafe {CStr::from_ptr(field)};
    let field = field.to_bytes();
    let field = unsafe{str::from_utf8_unchecked(field)};

    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { cache_with_field(pfrom, pto, field, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn get_with_field_c(page_id: libc::c_int, pfrom: libc::c_int, pto: libc::c_int, field: *const libc::c_char, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let page_id = page_id as usize;
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let field = unsafe {CStr::from_ptr(field)};
    let field = field.to_bytes();
    let field = unsafe{str::from_utf8_unchecked(field)};

    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { get_with_field(page_id, pfrom, pto, field, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn cache_page_view_c(pfrom: libc::c_int, pto: libc::c_int, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { cache_page_view(pfrom, pto, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn get_page_view_c(page_id: libc::c_int, pfrom: libc::c_int, pto: libc::c_int, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let page_id = page_id as usize;
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { get_page_view(page_id, pfrom, pto, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn cache_total_page_view_c(from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { cache_total_page_view(from, to, unique) };
}

#[no_mangle]
pub extern "C" fn get_total_page_view_c(page_id: libc::c_int, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let page_id = page_id as usize;
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { get_total_page_view(page_id, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn dealloc_str(s: *mut libc::c_char) {
    unsafe {CString::from_raw(s)};

}
