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
use hyper::status::StatusCode;

use std::*;
use std::io::{Write,Read,stderr};
use std::borrow::{Borrow, Cow};
use std::error::Error;
use std::ffi::{CStr, CString};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use regex::Regex;
use serde_json::{Value, to_string, from_str, from_reader, from_value, to_value};
use serde::{Deserialize, Deserializer, Serializer, Serialize};

use redis::Commands;

const RESULT_LENGTH: usize = 30;
const PRE_TRIM_NUM: usize = 30;

macro_rules! get_field {
    ($obj: expr, $field: expr) => {
        {
            let v = $obj.remove($field);
            let v = try!(v.ok_or(D::Error::missing_field($field)));
            try!(from_value(v).map_err(|e| D::Error::syntax(&format!("{:?}", e))))
        }
    }
}

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

#[derive(Debug)]
struct KeenResult<C> {
    result: Vec<C>
}

impl<C> Deserialize for KeenResult<C> where C: Deserialize {
    fn deserialize<D>(deserializer: &mut D) -> Result<KeenResult<C>, D::Error> where D: Deserializer {
        use serde::de::Error;
        let mut bt: BTreeMap<String, Vec<C>> = try!(BTreeMap::deserialize(deserializer));
        if let Some(result) = bt.remove("result") {
            Ok(KeenResult {
                result: result
            })
        } else {
            Err(D::Error::missing_field("result"))
        }
    }
}

impl<C> Serialize for KeenResult<C> where C: Serialize {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(vec![("result", &self.result)].into_iter(), Some(1)))
    }
}

#[derive(Debug)]
struct Day<V> {
    value: V,
    timeframe: Timeframe
}

impl<V> Deserialize for Day<V> where V: Deserialize {
    fn deserialize<D>(deserializer: &mut D) -> Result<Day<V>, D::Error> where D: Deserializer {
        use serde::de::Error;
        let mut object: BTreeMap<String, Value> = try!(Deserialize::deserialize(deserializer));
        let value = get_field!(object, "value");
        let timeframe = get_field!(object, "timeframe");

        Ok(Day {
            value: value,
            timeframe: timeframe
        })
    }
}

impl<V> Serialize for Day<V> where V: Serialize {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(
            vec![("value", to_value(&self.value)),
                ("timeframe", to_value(&self.timeframe))].into_iter(), Some(2)))
    }
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
            .and_then(|f| f.as_string().or(f.as_null().map(|_| "null".into())).map(|f| Group::Referrer(f.into())));
        let country = object.remove("ip_geo_info.country")
            .and_then(|f| f.as_string().or(f.as_null().map(|_| "null".into())).map(|f| Group::Country(f.into())));
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

#[derive(Debug)]
struct Timeframe {
    start: String,
    end: String
}

impl Deserialize for Timeframe {
    fn deserialize<D>(deserializer: &mut D) -> Result<Timeframe, D::Error> where D: Deserializer {
        use serde::de::Error;
        let mut object: BTreeMap<String, String> = try!(BTreeMap::deserialize(deserializer));
        let start = try!(object.remove("start").ok_or(D::Error::missing_field("no such field: start")));
        let end = try!(object.remove("end").ok_or(D::Error::missing_field("no such field: end")));

        Ok(Timeframe {
            start: start,
            end: end
        })
    }
}

impl Serialize for Timeframe {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(
            vec![("start", self.start.clone()), ("end", self.end.clone())].into_iter(), Some(2)))
    }
}

#[derive(Debug)]
pub struct KeenError {
    message: String,
    error_code: String
}

impl Deserialize for KeenError {
    fn deserialize<D>(deserializer: &mut D) -> Result<KeenError, D::Error> where D: Deserializer {
        use serde::de::Error;
        let mut object: BTreeMap<String, String> = try!(BTreeMap::deserialize(deserializer));
        Ok(KeenError {
            message: try!(object.remove("message").ok_or(D::Error::missing_field("no such field: message"))),
            error_code: try!(object.remove("error_code").ok_or(D::Error::missing_field("no such field: error_code")))
        })
    }
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
        TimeError(chrono::ParseError),
        /// keen error
        KeenError(KeenError)
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
            &KeenError(ref s) => s.description(),
        }
    }
}

pub type NativeResult<T> = Result<T, NativeError>;

fn open_redis() -> NativeResult<redis::Connection> {
    let c = try!(env::var("REDISCLOUD_URL"));
    Ok(try!(redis::Client::open(&c[..]).and_then(|client| client.get_connection())))
}

const TIMEOUT: usize = 360;

fn generate_keen_client() -> NativeResult<KeenClient> {
    let keen_project = try!(env::var("KEEN_PROJECT_ID"));
    let keen_read_key = try!(env::var("KEEN_READ_KEY"));

    let mut client = KeenClient::new(&keen_read_key, &keen_project);
    client.timeout(time::Duration::from_secs(TIMEOUT as u64));
    Ok(client)
}
fn generate_interval(i: Option<&str>) -> Option<Interval> {
    i.and_then(|s| {
        match s {
            "hourly" => Some(Interval::Hourly),
            "daily" => Some(Interval::Daily),
            _ => None
        }
    })
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
    static ref SPIDER_FILTER: Filter = Filter::ne("parsed_user_agent.device.family", "Spider");
}

const COLLECTION: &'static str = "strikingly_pageviews";
const TTL: usize = 48 * 60 * 60;
const UTC_CORRECTION: i64 = 4;

fn utc_correction(time: DateTime<UTC>, hours: i64) -> DateTime<UTC> {
    time + Duration::hours(hours)
}

fn date_to_string(time: DateTime<UTC>, hours: i64) -> String {
    utc_correction(time, hours).date().and_hms(0,0,0).to_rfc3339()
}

pub fn cache_page_view_range(pfrom: usize, pto: usize, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool, interval: Option<Interval>) -> NativeResult<&'static str> {
    let client = try!(generate_keen_client());
    let redis = try!(open_redis());

    let from_s = date_to_string(from, UTC_CORRECTION);
    let to_s = date_to_string(to, UTC_CORRECTION);

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some("pageId"), &from_s, &to_s, interval.clone(), Some((pfrom, pto)));

    let m = if unique {Metric::CountUnique("ip_address".into())} else {Metric::Count};
    let t = TimeFrame::Absolute(from, to);
    let c = COLLECTION.into();

    let mut q = client.query(m, c, t);

    interval.clone().map(|i| q.interval(i));
    q.filter(SPIDER_FILTER.clone());
    q.group_by("pageId");
    q.filter(Filter::gt("pageId", pfrom));
    q.filter(Filter::lte("pageId", pto));

    debug!("cache_page_view_range: url: {}", q.url());

    let mut resp = try!(timeit!(q.data(), "get data from keen io"));
    if resp.status != StatusCode::Ok {
        let e: KeenError = try!(from_reader(resp));
        return Err(NativeError::KeenError(e));
    }

    let mut v = vec![];
    let _ = resp.read_to_end(&mut v);
    let mut s = unsafe {String::from_utf8_unchecked(v)};

    if interval.is_some() {
        let mut result: KeenResult<Day<Vec<Page>>> = try!(from_str(&s));
        result.result.iter_mut().foreach(|day| day.value.retain(|p| p.result != 0));
        s = try!(to_string(&result));
    } else {
        let mut result: KeenResult<Page> = try!(from_str(&s));
        result.result.retain(|p| p.result != 0);
        s = try!(to_string(&result));
    }

    let _ = try!(redis.set(&key[..], s));
    let _ = try!(redis.expire(&key[..], TTL));
    Ok(r#""ok""#)
}

pub fn get_page_view_range(page_id: usize, pfrom: usize, pto: usize, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool, interval: Option<Interval>) -> NativeResult<String> {
    let redis = try!(open_redis());
    let from_s = date_to_string(from, 0);
    let to_s = date_to_string(to, 0);

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some("pageId"), &from_s, &to_s, interval.clone(), Some((pfrom, pto)));

    let s: String = try!(redis.get(&key[..]));

    if interval.is_some() {
        let result: KeenResult<Day<Vec<Page>>> = try!(from_str(&s));
        let ds: Vec<_> = result.result.into_iter().map(|day| {
            Day {
                value: day.value.into_iter().find(|p| p.page_id == page_id).map(|s| s.result).unwrap_or(0) as usize,
                timeframe: day.timeframe
            }
        }).collect();
        let s = try!(to_string(&ds));
        Ok(Regex::new(&format!(r#","pageId":{}"#, page_id)).unwrap().replace_all(&s, ""))
    } else {
        let re = Regex::new(&format!(r#"\{{"pageId": ?{}, ?"result": ?(\d+)\}}"#, page_id)).unwrap();
        Ok(format!("{}", re.captures(&s).and_then(|g| {
            g.at(1).and_then(|d| d.parse().ok())
        }).unwrap_or(0)))
    }
}

pub fn cache_with_field_range(pfrom: usize, pto: usize, field: &str, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<&'static str> {
    let client = try!(generate_keen_client());
    let redis = try!(open_redis());

    let from_s = date_to_string(from, UTC_CORRECTION);
    let to_s = date_to_string(to, UTC_CORRECTION);

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some(field), &from_s, &to_s, None, Some((pfrom, pto)));

    let m = if unique {Metric::CountUnique("ip_address".into())} else {Metric::Count};
    let t = TimeFrame::Absolute(from.clone(), to.clone());
    let c = COLLECTION.into();

    let mut q = client.query(m, c, t);
    q.filter(SPIDER_FILTER.clone());
    q.group_by("pageId");
    q.group_by(field);
    q.filter(Filter::gt("pageId", pfrom));
    q.filter(Filter::lte("pageId", pto));

    debug!("cache_with_field_range: url: {}", q.url());

    let mut resp = try!(timeit!(q.data(), "get data from keen io"));
    if resp.status != StatusCode::Ok {
        let e: KeenError = try!(from_reader(resp));
        return Err(NativeError::KeenError(e));
    }

    let mut v = vec![];
    let _ = resp.read_to_end(&mut v);
    let s = unsafe {String::from_utf8_unchecked(v)};

    let mut result: KeenResult<Page> = try!(from_str(&s));

    result.result = result.result.into_iter().filter(|p| p.result != 0)
        .group_by(|p| p.page_id)
        .flat_map(|(pid, mut records)| {
            if records.len() > 1 {
                records.sort_by(|a,b| b.result.cmp(&a.result));
                let (less, more): (Vec<_>, Vec<_>) = records.into_iter().enumerate().partition(|&(idx, _)| idx < PRE_TRIM_NUM);

                let result = more.into_iter().fold(0, |acc, e| acc + e.1.result);
                let mut less: Vec<_> = less.into_iter().map(|e| e.1).collect();

                if result != 0 {
                    let others_group = match less[0].group {
                        Group::Country(_)   => Group::Country("others".into()),
                        Group::Referrer(_)  => Group::Referrer("others".into()),
                        Group::None         => {
                        let _ = writeln!(stderr(), "unreachable branch: error! {:?}, {:?}", less[0], less);
                            Group::None
                        }
                    };

                    less.push(Page {
                        result: result,
                        page_id: pid,
                        group: others_group
                    });
                }
                less.into_iter()
            } else {
                records.into_iter()
            }
        }).collect();


    let s = to_string(&result).unwrap();
    let _ = try!(redis.set(&key[..], s));
    let _ = try!(redis.expire(&key[..], TTL));
    Ok(r#""ok""#)
}

pub fn get_with_field_range(page_id: usize, pfrom: usize, pto: usize, field: &str, from: DateTime<UTC>, to: DateTime<UTC>, unique: bool) -> NativeResult<String> {
    let redis = try!(open_redis());

    let from_s = date_to_string(from, 0);
    let to_s = date_to_string(to, 0);

    let metric = if unique {"count_unique"} else {"count"};
    let key = generate_redis_key(metric, Some(field), &from_s, &to_s, None, Some((pfrom, pto)));

    let s: String = try!(redis.get(&key[..]));

    let mut result: KeenResult<Page> = try!(from_str(&s));

    result.result.retain(|p| p.page_id == page_id);
 
    let s = to_string(&result.result).unwrap();
    Ok(Regex::new(&format!(r#","pageId":{}"#, page_id)).unwrap().replace_all(&s, ""))
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
    let s = format!("{}", s).replace("\"", "'");
    CString::new(format!(r#"{{"error": "{}"}}"#, s)).unwrap().into_raw() as *const _
}

fn make_result<D: Display>(r: D) -> *const libc::c_char {
    CString::new(format!(r#"{{"result": {}}}"#, r)).unwrap().into_raw() as *const _
}

#[no_mangle]
pub extern "C" fn cache_with_field_range_c(pfrom: libc::c_int, pto: libc::c_int, field: *const libc::c_char, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let field = unsafe {CStr::from_ptr(field)};
    let field = field.to_bytes();
    let field = unsafe{str::from_utf8_unchecked(field)};

    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };

    returna! { cache_with_field_range(pfrom, pto, field, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn get_with_field_range_c(page_id: libc::c_int, pfrom: libc::c_int, pto: libc::c_int, field: *const libc::c_char, from: *const libc::c_char, to: *const libc::c_char, unique: bool) -> *const libc::c_char {
    let page_id = page_id as usize;
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let field = unsafe {CStr::from_ptr(field)};
    let field = field.to_bytes();
    let field = unsafe{str::from_utf8_unchecked(field)};

    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    returna! { get_with_field_range(page_id, pfrom, pto, field, from, to, unique) };
}

#[no_mangle]
pub extern "C" fn cache_page_view_range_c(pfrom: libc::c_int, pto: libc::c_int, from: *const libc::c_char, to: *const libc::c_char, unique: bool, interval: *const libc::c_char) -> *const libc::c_char {
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    let interval = unsafe {CStr::from_ptr(interval)};
    let interval = interval.to_bytes();
    let interval = unsafe{str::from_utf8_unchecked(interval)};
    let interval = generate_interval(Some(interval));
    returna! { cache_page_view_range(pfrom, pto, from, to, unique, interval) };
}

#[no_mangle]
pub extern "C" fn get_page_view_range_c(page_id: libc::c_int, pfrom: libc::c_int, pto: libc::c_int, from: *const libc::c_char, to: *const libc::c_char, unique: bool, interval: *const libc::c_char) -> *const libc::c_char {
    let page_id = page_id as usize;
    let pfrom = pfrom as usize;
    let pto = pto as usize;
    let from = returne! { parse_day(from) };
    let to = returne! { parse_day(to) };
    let interval = unsafe {CStr::from_ptr(interval)};
    let interval = interval.to_bytes();
    let interval = unsafe{str::from_utf8_unchecked(interval)};
    let interval = generate_interval(Some(interval));
    returna! { get_page_view_range(page_id, pfrom, pto, from, to, unique, interval) };
}

#[no_mangle]
pub extern "C" fn dealloc_str(s: *mut libc::c_char) {
    unsafe {CString::from_raw(s)};
}

