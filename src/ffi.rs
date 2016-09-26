use std::ptr;
use std::ffi::{CString, CStr};
use std::any::Any;
use std::time::Duration;
use std::sync::Mutex;
use std::cell::RefCell;
use std::error::Error as StdError;

use libc::c_char;
use libc::c_int;

use keen::*;
use protocol::*;
use chrono::UTC;

use client::*;
use errors::{Error, Result};

macro_rules! cstr {
    ($var: expr) => (unsafe { CStr::from_ptr($var).to_str().unwrap() })
}

lazy_static!{
    static ref ERROR: Mutex<RefCell<Option<String>>> = Mutex::new(RefCell::new(None));
}

fn set_global_error(e: Error) {
    let l = ERROR.lock().unwrap();
    *l.borrow_mut() = Some(format!("{}", e.description()));
}

// it is FFIBox(*mut Box<T>)
#[repr(C)]
pub struct FFICacheClient(*mut Box<Any>);

impl FFICacheClient {
    fn new(t: KeenCacheClient) -> FFICacheClient {
        FFICacheClient(Box::into_raw(Box::new(Box::new(t) as Box<Any>)))
    }
    fn as_mut(&mut self) -> &mut KeenCacheClient {
        (unsafe { &mut *self.0 }).downcast_mut::<KeenCacheClient>().unwrap()
    }
    fn drop(self) {
        unsafe { Box::from_raw(self.0) };
    }
}

impl From<KeenCacheClient> for FFICacheClient {
    fn from(r: KeenCacheClient) -> Self {
        Self::new(r)
    }
}

// it is FFIBox(*mut Box<T>)
#[repr(C)]
pub struct FFICacheQuery(*mut Box<Any>);

impl FFICacheQuery {
    fn new(t: KeenCacheQuery) -> FFICacheQuery {
        FFICacheQuery(Box::into_raw(Box::new(Box::new(t) as Box<Any>)))
    }
    fn null() -> FFICacheQuery {
        FFICacheQuery(ptr::null_mut())
    }
    fn as_mut(&mut self) -> &mut KeenCacheQuery {
        (unsafe { &mut *self.0 }).downcast_mut::<KeenCacheQuery>().unwrap()
    }
    fn as_ref(&self) -> &KeenCacheQuery {
        (unsafe { &*self.0 }).downcast_ref::<KeenCacheQuery>().unwrap()
    }
    fn drop(self) {
        unsafe { Box::from_raw(self.0) };
    }
}

impl From<KeenCacheQuery> for FFICacheQuery {
    fn from(r: KeenCacheQuery) -> Self {
        Self::new(r)
    }
}


// it is FFIBox(*mut Box<Option<T>>)
#[repr(C)]
pub struct FFICacheResult(*mut Box<Any>);

impl FFICacheResult {
    fn new<T: Any>(t: KeenCacheResult<T>) -> FFICacheResult {
        FFICacheResult(Box::into_raw(Box::new(Box::new(Some(t)) as Box<Any>)))
    }
    fn null() -> FFICacheResult {
        FFICacheResult(ptr::null_mut())
    }
    fn is<T: Any>(&self) -> bool {
        (unsafe { &mut *self.0 })
            .downcast_mut::<Option<KeenCacheResult<T>>>()
            .is_some()
    }
    fn take<T: Any>(self) -> Option<KeenCacheResult<T>> {
        (unsafe { &mut *self.0 })
            .downcast_mut::<Option<KeenCacheResult<T>>>()
            .map(|x| x.take().unwrap())
    }
}

impl Drop for FFICacheResult {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.0) };
        self.0 = 0 as *mut _;
    }
}

impl<T: Any> From<KeenCacheResult<T>> for FFICacheResult {
    fn from(r: KeenCacheResult<T>) -> Self {
        Self::new(r)
    }
}



// ----------------  apis  -----------------
#[no_mangle]
pub extern "C" fn new_client(key: *mut c_char, project: *mut c_char) -> FFICacheClient {
    let key = cstr!(key);
    let project = cstr!(project);
    KeenCacheClient::new(key, project).into()
}

#[no_mangle]
pub extern "C" fn set_redis(mut c: FFICacheClient, url: *mut c_char) -> bool {
    let url = cstr!(url);
    let result: Result<()> = c.as_mut().set_redis(url);
    match result {
        Ok(_) => true,
        Err(e) => {
            set_global_error(e);
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn set_timeout(mut c: FFICacheClient, sec: c_int) -> bool {
    c.as_mut().set_timeout(Duration::new(sec as u64, 0));
    true
}

pub const COUNT: c_int = 0;
pub const COUNT_UNIQUE: c_int = 1;

#[no_mangle]
pub extern "C" fn new_query(mut c: FFICacheClient,
                            metric_type: c_int,
                            metric_target: *mut c_char,
                            collection: *mut c_char,
                            start: *mut c_char,
                            end: *mut c_char)
                            -> FFICacheQuery {

    let metric = match metric_type {
        COUNT => Metric::Count,
        COUNT_UNIQUE => Metric::CountUnique(cstr!(metric_target).into()),
        _ => {
            set_global_error(format!("unsupported metric type '{}'", metric_type).into());
            return FFICacheQuery::null();
        }
    };
    let collection = cstr!(collection);
    let start = cstr!(start).parse().unwrap_or(UTC::now());
    let end = cstr!(end).parse().unwrap_or(UTC::now());
    let query = c.as_mut().query(metric, collection.into(), TimeFrame::Absolute(start, end));
    query.into()
}

#[no_mangle]
pub extern "C" fn group_by(mut q: FFICacheQuery, group: *mut c_char) -> bool {
    let group = cstr!(group);
    q.as_mut().group_by(group);
    true
}

pub const EQ: c_int = 0;
pub const LT: c_int = 1;
pub const GT: c_int = 2;
pub const LTE: c_int = 3;
pub const GTE: c_int = 4;
pub const IN: c_int = 5;
pub const NE: c_int = 6;

fn gen_filter<U>(filter_a: &str, filter_b: U, filter_type: c_int) -> Result<Filter>
    where U: ToFilterValue
{
    let filter = match filter_type {
        EQ => Filter::eq(filter_a, filter_b),
        LT => Filter::lt(filter_a, filter_b),
        GT => Filter::gt(filter_a, filter_b),
        GTE => Filter::gte(filter_a, filter_b),
        LTE => Filter::lte(filter_a, filter_b),
        IN => Filter::isin(filter_a, filter_b),
        NE => Filter::ne(filter_a, filter_b),
        _ => {
            return Err(format!("unsupported filter type '{}'", filter_type).into());
        }
    };
    return Ok(filter);
}

#[no_mangle]
pub extern "C" fn filter(mut q: FFICacheQuery,
                         filter_type: c_int,
                         filter_a: *mut c_char,
                         filter_b: *mut c_char)
                         -> bool {
    let filter_a = cstr!(filter_a);
    let filter_b = cstr!(filter_b);
    if let Ok(i) = filter_b.parse() {
        // int
        let filter_b: i64 = i;
        match gen_filter(filter_a, filter_b, filter_type) {
            Ok(filter) => {
                q.as_mut().filter(filter);
                return true;
            }
            Err(e) => {
                set_global_error(e);
                return false;
            }
        }
    } else if filter_b.ends_with(']') && filter_b.starts_with('[') {
        // vec
        let filter_b = filter_b.trim_matches('[').trim_matches(']');

        if filter_b.split(',')
            .map(|c| c.trim())
            .find(|c| c.starts_with('"') && c.ends_with('"'))
            .is_some() {
            // string vec
            let iter = filter_b.split(',').map(|c| c.trim().trim_matches('"'));
            let filter_b: Vec<_> = iter.collect();
            match gen_filter(filter_a, filter_b, filter_type) {
                Ok(filter) => {
                    q.as_mut().filter(filter);
                    return true;
                }
                Err(e) => {
                    set_global_error(e);
                    return false;
                }
            }

        } else {
            // int vec
            let iter = filter_b.split(',').map(|c| c.trim());
            let filter_b: Vec<_> = iter.map(|c| c.parse::<i64>().ok().unwrap_or_default())
                .collect();
            match gen_filter(filter_a, filter_b, filter_type) {
                Ok(filter) => {
                    q.as_mut().filter(filter);
                    return true;
                }
                Err(e) => {
                    set_global_error(e);
                    return false;
                }
            }
        }
    } else {
        // string
        match gen_filter(filter_a, filter_b, filter_type) {
            Ok(filter) => {
                q.as_mut().filter(filter);
                return true;
            }
            Err(e) => {
                set_global_error(e);
                return false;
            }
        }
    }
}

const MINUTELY: c_int = 0;
const HOURLY: c_int = 1;
const DAILY: c_int = 2;
const WEEKLY: c_int = 3;
const MONTHLY: c_int = 4;
const YEARLY: c_int = 5;

#[no_mangle]
pub extern "C" fn interval(mut q: FFICacheQuery, interval: c_int) -> bool {
    match interval {
        MINUTELY => q.as_mut().interval(Interval::Minutely),
        HOURLY => q.as_mut().interval(Interval::Hourly),
        DAILY => q.as_mut().interval(Interval::Daily),
        WEEKLY => q.as_mut().interval(Interval::Weekly),
        MONTHLY => q.as_mut().interval(Interval::Monthly),
        YEARLY => q.as_mut().interval(Interval::Yearly),
        _ => {
            set_global_error(format!("unsupported interval type '{}'", interval).into());
            return false;
        }
    }
    true
}

#[no_mangle]
pub extern "C" fn other(mut q: FFICacheQuery, key: *mut c_char, value: *mut c_char) -> bool {
    let key = cstr!(key);
    let value = cstr!(value);
    q.as_mut().other(key, value);
    true
}

pub const POD: c_int = 0;
pub const ITEMS: c_int = 1;
pub const DAYSPOD: c_int = 2;
pub const DAYSITEMS: c_int = 3;

#[no_mangle]
pub extern "C" fn send_query(q: FFICacheQuery) -> FFICacheResult {
    use ::client::ResultType;

    let r = match q.as_ref().tp {
        ResultType::POD => {
            let r: KeenCacheResult<i64> = match q.as_ref().data() {
                Ok(s) => s,
                Err(e) => {
                    set_global_error(format!("data type can not be converted to i64: '{}'", e)
                        .into());
                    return FFICacheResult::null();
                }
            };
            r.into()
        }
        ResultType::Items => {
            let r: KeenCacheResult<Items> = match q.as_ref().data() {
                Ok(s) => s,
                Err(e) => {
                    set_global_error(format!("data type can not be converted to Items: '{}'", e)
                        .into());
                    return FFICacheResult::null();
                }
            };
            r.into()
        }
        ResultType::DaysPOD => {
            let r: KeenCacheResult<Days<i64>> = match q.as_ref().data() {
                Ok(s) => s,
                Err(e) => {
                    set_global_error(format!("data type can not be converted to Days<i64>: '{}'",
                                             e)
                        .into());
                    return FFICacheResult::null();
                }
            };
            r.into()
        }
        ResultType::DaysItems => {
            let r: KeenCacheResult<Days<Items>> = match q.as_ref().data() {
                Ok(s) => s,
                Err(e) => {
                    set_global_error(format!("data type can not be converted to Days<Items>: \
                                              '{}'",
                                             e)
                        .into());

                    return FFICacheResult::null();
                }
            };
            r.into()
        }
    };
    r
}

// consume
#[no_mangle]
pub extern "C" fn accumulate(r: FFICacheResult, to: c_int) -> FFICacheResult {
    if r.is::<i64>() {
        set_global_error(format!("i64 can not be converted to others").into());
        return FFICacheResult::null();
    } else if r.is::<Items>() {
        let r: KeenCacheResult<i64> = r.take::<Items>().unwrap().accumulate();
        r.into()
    } else if r.is::<Days<i64>>() {
        let r: KeenCacheResult<i64> = r.take::<Days<i64>>().unwrap().accumulate();
        r.into()
    } else if r.is::<Days<Items>>() {
        match to {
            DAYSPOD => {
                let r: KeenCacheResult<Days<i64>> = r.take::<Days<Items>>().unwrap().accumulate();
                r.into()
            }
            POD => {
                let r: KeenCacheResult<i64> = r.take::<Days<Items>>().unwrap().accumulate();
                r.into()
            }
            _ => {
                set_global_error(format!("data type can not be converted to '{}'", to).into());
                FFICacheResult::null()
            }
        }
    } else {
        set_global_error(format!("not a valid target type '{}'", to).into());
        FFICacheResult::null()
    }
}

// consume
#[no_mangle]
pub extern "C" fn range(r: FFICacheResult, from: *mut c_char, to: *mut c_char) -> FFICacheResult {
    let from = cstr!(from);
    let to = cstr!(to);
    let from = from.parse().unwrap();
    let to = to.parse().unwrap();

    if r.is::<i64>() {
        set_global_error(format!("i64 can not be converted to others").into());
        FFICacheResult::null()
    } else if r.is::<Items>() {
        set_global_error(format!("Items can not be converted to others").into());
        FFICacheResult::null()
    } else if r.is::<Days<i64>>() {
        let r = r.take::<Days<i64>>().unwrap().range(from, to);
        r.into()
    } else if r.is::<Days<Items>>() {
        let r = r.take::<Days<Items>>().unwrap().range(from, to);
        r.into()
    } else {
        set_global_error(format!("not a valid source type").into());
        FFICacheResult::null()
    }
}

// consume
#[no_mangle]
pub extern "C" fn select(r: FFICacheResult,
                         key: *mut c_char,
                         value: *mut c_char,
                         to: c_int)
                         -> FFICacheResult {
    let key = cstr!(key);
    let value = cstr!(value);
    if r.is::<i64>() {
        set_global_error(format!("i64 not support select").into());
        FFICacheResult::null()
    } else if r.is::<Items>() {
        match to {
            DAYSITEMS | DAYSPOD => {
                set_global_error(format!("Items can not be converted to Days<Items> or Days<i64>")
                    .into());
                FFICacheResult::null()
            }
            ITEMS => {
                let r: KeenCacheResult<Items> =
                    r.take::<Items>().unwrap().select((key, StringOrI64::String(value.into())));
                r.into()
            }
            POD => {
                let r: KeenCacheResult<i64> =
                    r.take::<Items>().unwrap().select((key, StringOrI64::String(value.into())));
                r.into()
            }
            _ => {
                set_global_error(format!("not a valid target type '{}'", to).into());
                FFICacheResult::null()
            }
        }
    } else if r.is::<Days<i64>>() {
        set_global_error(format!("i64 not support select").into());
        FFICacheResult::null()
    } else if r.is::<Days<Items>>() {
        let r = r.take::<Days<Items>>().unwrap();
        let param = (key, StringOrI64::String(value.into()));
        match to {
            DAYSITEMS => {
                let r: KeenCacheResult<Days<Items>> = r.select(param);
                r.into()
            }
            POD => {
                let r: KeenCacheResult<i64> = r.select(param);
                r.into()
            }
            DAYSPOD => {
                let r: KeenCacheResult<Days<i64>> = r.select(param);
                r.into()
            }
            _ => {
                set_global_error(format!("not a valid target type '{}'", to).into());
                FFICacheResult::null()
            }
        }
    } else {
        set_global_error(format!("not a valid source type").into());
        FFICacheResult::null()
    }
}

// consume
#[no_mangle]
pub extern "C" fn to_redis(r: FFICacheResult, key: *mut c_char, expire: c_int) -> bool {
    let expire = expire as u64;

    let key = cstr!(key);

    let result = if r.is::<i64>() {
        r.take::<i64>().unwrap().to_redis(key, expire)
    } else if r.is::<Items>() {
        r.take::<Items>().unwrap().to_redis(key, expire)
    } else if r.is::<Days<i64>>() {
        r.take::<Days<i64>>().unwrap().to_redis(key, expire)
    } else if r.is::<Days<Items>>() {
        r.take::<Days<Items>>().unwrap().to_redis(key, expire)
    } else {
        set_global_error(format!("not a valid source type").into());
        return false;
    };
    match result {
        Ok(_) => true,
        Err(e) => {
            set_global_error(format!("'{}'", e).into());
            false
        }
    }
}

// consume
#[no_mangle]
pub extern "C" fn to_string(r: FFICacheResult) -> *const c_char {
    let s = if r.is::<i64>() {
        r.take::<i64>().unwrap().to_string()
    } else if r.is::<Items>() {
        r.take::<Items>().unwrap().to_string()
    } else if r.is::<Days<i64>>() {
        r.take::<Days<i64>>().unwrap().to_string()
    } else if r.is::<Days<Items>>() {
        r.take::<Days<Items>>().unwrap().to_string()
    } else {
        set_global_error(format!("not a valid source type").into());
        return ptr::null();
    };
    CString::new(s).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn from_redis(url: *const c_char, key: *const c_char, tp: c_int) -> FFICacheResult {
    let key = cstr!(key);
    let url = cstr!(url);
    macro_rules! from_redis {
        ($t: ty) => {{
            let r: $t = match KeenCacheResult::from_redis(url, key) {
                Ok(o) => o,
                Err(e) => {
                    set_global_error(e);
                    return FFICacheResult::null();
                }
            };
            r.into()
        }}
    }

    match tp {
        POD => from_redis!(KeenCacheResult<i64>),
        ITEMS => from_redis!(KeenCacheResult<Items>),
        DAYSPOD => from_redis!(KeenCacheResult<Days<i64>>),
        DAYSITEMS => from_redis!(KeenCacheResult<Days<Items>>),
        _ => {
            set_global_error(format!("not a valid target type '{}'", tp).into());
            FFICacheResult::null()
        }
    }
}

#[no_mangle]
pub extern "C" fn free_string(s: *mut c_char) {
    unsafe { CString::from_raw(s) };
}

// consume
#[no_mangle]
pub extern "C" fn free_result(_: FFICacheResult) {}

// consume
#[no_mangle]
pub extern "C" fn free_query(q: FFICacheQuery) {
    q.drop();
}

// consume
#[no_mangle]
pub extern "C" fn free_client(c: FFICacheClient) {
    c.drop();
}

#[no_mangle]
pub extern "C" fn last_error() -> *mut c_char {
    let l = ERROR.lock().unwrap();
    let o = l.borrow_mut()
        .take();
    o.map(|e| CString::new(e).unwrap().into_raw())
        .unwrap_or(0 as *mut _)
}