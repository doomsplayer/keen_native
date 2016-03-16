use client::*;
use keen::*;
use std::ffi::CStr;
use libc::c_char;
use libc::c_int;
use std::mem::forget;
use error::NativeResult;
use protocol::*;
use chrono::UTC;
use std::mem::transmute;
use std::ptr;
use std::ffi::CString;

// leak
fn with<T, F, R>(c: *mut T, f: F) -> R
    where F: for<'a> FnOnce(&'a mut T) -> R
{
    let mut client = unsafe { Box::from_raw(c) };
    let result = f(&mut client);
    forget(client);
    result
}

#[no_mangle]
pub extern "C" fn new(key: *mut c_char, project: *mut c_char) -> *const KeenCacheClient {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap() };
    let project = unsafe { CStr::from_ptr(project).to_str().unwrap() };
    Box::into_raw(Box::new(KeenCacheClient::new(key, project)))
}

#[no_mangle]
pub extern "C" fn set_redis(c: *mut KeenCacheClient, url: *mut c_char) -> bool {
    with(c, |client| {
        let url = unsafe { CStr::from_ptr(url).to_str().unwrap() };
        let result: NativeResult<()> = client.set_redis(url);
        result.is_ok()
    })
}

#[no_mangle]
pub extern "C" fn set_timeout(c: *mut KeenCacheClient, sec: c_int) -> bool {
    use std::time::Duration;
    with(c, |c| {
        let d = Duration::new(sec as u64, 0);
        c.set_timeout(d);
        true
    })
}

pub const COUNT: c_int = 0;
pub const COUNT_UNIQUE: c_int = 1;

#[no_mangle]
pub extern "C" fn query<'a>(c: *mut KeenCacheClient,
                            metric_type: c_int,
                            metric_target: *mut c_char,
                            collection: *mut c_char,
                            start: *mut c_char,
                            end: *mut c_char)
                            -> *const KeenCacheQuery<'a> {
    with(c, |c| {
        let metric = match metric_type {
            COUNT => Metric::Count,
            COUNT_UNIQUE => {
                let target = unsafe { CStr::from_ptr(metric_target).to_str().unwrap() };
                Metric::CountUnique(target.into())
            }
            _ => unimplemented!(),
        };
        let collection = unsafe { CStr::from_ptr(collection).to_str().unwrap() };
        let start = unsafe { CStr::from_ptr(start).to_str().unwrap() };
        let end = unsafe { CStr::from_ptr(end).to_str().unwrap() };
        let start = start.parse().unwrap_or(UTC::now());
        let end = end.parse().unwrap_or(UTC::now());
        let q = unsafe {
            transmute(c.query(metric, collection.into(), TimeFrame::Absolute(start, end)))
        };
        Box::into_raw(Box::new(q))
    })
}

#[no_mangle]
pub extern "C" fn group_by(q: *mut KeenCacheQuery, group: *mut c_char) -> bool {
    with(q, |q| {
        let group = unsafe { CStr::from_ptr(group).to_str().unwrap() };
        q.group_by(group);
        true
    })
}

pub const EQ: c_int = 0;
pub const LT: c_int = 1;
pub const GT: c_int = 2;
pub const LTE: c_int = 3;
pub const GTE: c_int = 4;
pub const IN: c_int = 5;
pub const NE: c_int = 6;

fn gen_filter<U>(filter_a: &str, filter_b: U, filter_type: c_int) -> Option<Filter>
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
            warn!("unsupported filter: {}", filter_type);
            return None;
        }
    };
    return Some(filter);
}

#[no_mangle]
pub extern "C" fn filter(q: *mut KeenCacheQuery,
                         filter_type: c_int,
                         filter_a: *mut c_char,
                         filter_b: *mut c_char)
                         -> bool {
    with(q, |q| {
        let filter_a = unsafe { CStr::from_ptr(filter_a).to_str().unwrap() };
        let filter_b = unsafe { CStr::from_ptr(filter_b).to_str().unwrap() };
        if let Ok(i) = filter_b.parse() {
            // int
            let filter_b: i64 = i;
            if let Some(filter) = gen_filter(filter_a, filter_b, filter_type) {
                q.filter(filter);
                return true;
            } else {
                return false;
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
                if let Some(filter) = gen_filter(filter_a, filter_b, filter_type) {
                    q.filter(filter);
                    return true;
                } else {
                    return false;
                }
            } else {
                // int vec
                let iter = filter_b.split(',').map(|c| c.trim());
                let filter_b: Vec<_> = iter.map(|c| c.parse::<i64>().ok().unwrap_or_default())
                                           .collect();
                if let Some(filter) = gen_filter(filter_a, filter_b, filter_type) {
                    q.filter(filter);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            // string
            if let Some(filter) = gen_filter(filter_a, filter_b, filter_type) {
                q.filter(filter);
                return true;
            } else {
                return false;
            }
        }
    })
}

const MINUTELY: c_int = 0;
const HOURLY: c_int = 1;
const DAILY: c_int = 2;
const WEEKLY: c_int = 3;
const MONTHLY: c_int = 4;
const YEARLY: c_int = 5;

#[no_mangle]
pub extern "C" fn interval(q: *mut KeenCacheQuery, interval: c_int) -> bool {
    with(q, |q| {
        match interval {
            MINUTELY => q.interval(Interval::Minutely),
            HOURLY => q.interval(Interval::Hourly),
            DAILY => q.interval(Interval::Daily),
            WEEKLY => q.interval(Interval::Weekly),
            MONTHLY => q.interval(Interval::Monthly),
            YEARLY => q.interval(Interval::Yearly),
            _ => {
                warn!("unsupported interval: {}", interval);
                return false;
            }
        }
        true
    })
}

#[no_mangle]
pub extern "C" fn other(q: *mut KeenCacheQuery, key: *mut c_char, value: *mut c_char) -> bool {
    with(q, |q| {
        let key = unsafe { CStr::from_ptr(key).to_str().unwrap() };
        let value = unsafe { CStr::from_ptr(value).to_str().unwrap() };
        q.other(key, value);
        true
    })
}

pub const POD: c_int = 0;
pub const ITEMS: c_int = 1;
pub const DAYSPOD: c_int = 2;
pub const DAYSITEMS: c_int = 3;

#[no_mangle]
pub extern "C" fn data<'a>(q: *mut KeenCacheQuery, tp: c_int) -> *const KeenCacheResult<()> {
    let q = unsafe { Box::from_raw(q) };
    let r = match tp {
        POD => {
            let mut r: KeenCacheResult<i64> = match q.data() {
                Ok(s) => s,
                Err(e) => {
                    warn!("data type can not be converted to: target: i64, {}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        ITEMS => {
            let mut r: KeenCacheResult<Items> = match q.data() {
                Ok(s) => s,
                Err(e) => {
                    warn!("data type can not be converted to: target: Items, {}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        DAYSPOD => {
            let mut r: KeenCacheResult<Days<i64>> = match q.data() {
                Ok(s) => s,
                Err(e) => {
                    warn!("data type can not be converted to: target: Days<i64>, {}",
                          e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        DAYSITEMS => {
            let mut r: KeenCacheResult<Days<Items>> = match q.data() {
                Ok(s) => s,
                Err(e) => {
                    warn!("data type can not be converted to: target: Days<Items>, {}",
                          e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        _ => {
            warn!("unsupported type: {}", tp);
            return ptr::null();
        }
    };
    r
}

#[no_mangle]
pub extern "C" fn accumulate<'a>(r: *mut KeenCacheResult<()>,
                                 to: c_int)
                                 -> *const KeenCacheResult<()> {
    let r = unsafe { Box::from_raw(r) };
    match r.type_tag {
        ResultType::POD => {
            let _: Box<KeenCacheResult<Items>> = unsafe { transmute(r) }; // we need to transmute it or there will be leak
            warn!("cannot convert pod");
            return ptr::null();
        }
        ResultType::ITEMS => {
            let r: Box<KeenCacheResult<Items>> = unsafe { transmute(r) };
            let mut r: KeenCacheResult<i64> = r.accumulate();
            r.tt();
            return unsafe { transmute(Box::into_raw(Box::new(r))) };
        }
        ResultType::DAYSPOD => {
            let r: Box<KeenCacheResult<Days<i64>>> = unsafe { transmute(r) };
            let mut r: KeenCacheResult<i64> = r.accumulate();
            r.tt();
            return unsafe { transmute(Box::into_raw(Box::new(r))) };
        }
        ResultType::DAYSITEMS => {
            let r: Box<KeenCacheResult<Days<Items>>> = unsafe { transmute(r) };
            match to {
                DAYSPOD => {
                    let mut r: KeenCacheResult<Days<i64>> = r.accumulate();
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                POD => {
                    let mut r: KeenCacheResult<i64> = r.accumulate();
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                _ => {
                    warn!("target type cannot be converted to: {}", to);
                    return ptr::null();
                }
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn range<'a>(r: *mut KeenCacheResult<()>,
                            from: *mut c_char,
                            to: *mut c_char)
                            -> *const KeenCacheResult<()> {
    let from = unsafe { CStr::from_ptr(from).to_str().unwrap() };
    let to = unsafe { CStr::from_ptr(to).to_str().unwrap() };
    let from = from.parse().unwrap();
    let to = to.parse().unwrap();

    let r = unsafe { Box::from_raw(r) };
    match r.type_tag {
        ResultType::POD => {
            let _: Box<KeenCacheResult<i64>> = unsafe { transmute(r) };
            warn!("cannot use range on pod");
            return ptr::null();
        }
        ResultType::ITEMS => {
            let _: Box<KeenCacheResult<Items>> = unsafe { transmute(r) };
            warn!("cannot use range on items");
            return ptr::null();
        }
        ResultType::DAYSPOD => {
            let r: Box<KeenCacheResult<Days<i64>>> = unsafe { transmute(r) };
            let r = r.range(from, to);
            return unsafe { transmute(Box::into_raw(Box::new(r))) };
        }
        ResultType::DAYSITEMS => {
            let r: Box<KeenCacheResult<Days<Items>>> = unsafe { transmute(r) };
            let r = r.range(from, to);
            return unsafe { transmute(Box::into_raw(Box::new(r))) };
        }
    }
}

#[no_mangle]
pub extern "C" fn select<'a>(r: *mut KeenCacheResult<()>,
                             key: *mut c_char,
                             value: *mut c_char,
                             to: c_int)
                             -> *const KeenCacheResult<()> {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap() };
    let value = unsafe { CStr::from_ptr(value).to_str().unwrap() };
    let r = unsafe { Box::from_raw(r) };
    match r.type_tag {
        ResultType::POD => {
            let _: Box<KeenCacheResult<i64>> = unsafe { transmute(r) };
            warn!("cannot select on pod");
            return ptr::null();
        }
        ResultType::ITEMS => {
            let r: Box<KeenCacheResult<Items>> = unsafe { transmute(r) };
            match to {
                DAYSITEMS | DAYSPOD => {
                    warn!("cannot select to Days<_>");
                    return ptr::null();
                }
                ITEMS => {
                    let mut r: KeenCacheResult<Items> =
                        r.select((key, StringOrI64::String(value.into())));
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                POD => {
                    let mut r: KeenCacheResult<i64> = r.select((key,
                                                                StringOrI64::String(value.into())));
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                _ => {
                    warn!("target data type unexpected: {}", to);
                    return ptr::null();
                }
            }
        }
        ResultType::DAYSPOD => {
            let _: Box<KeenCacheResult<Days<i64>>> = unsafe { transmute(r) };
            warn!("cannot select on Days<i64>");
            return ptr::null();
        }
        ResultType::DAYSITEMS => {
            let r: Box<KeenCacheResult<Days<Items>>> = unsafe { transmute(r) };
            match to {
                DAYSITEMS => {
                    let mut r: KeenCacheResult<Days<Items>> =
                        r.select((key, StringOrI64::String(value.into())));
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                POD => {
                    let mut r: KeenCacheResult<i64> = r.select((key,
                                                                StringOrI64::String(value.into())));
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                DAYSPOD => {
                    let mut r: KeenCacheResult<Days<i64>> =
                        r.select((key, StringOrI64::String(value.into())));
                    r.tt();
                    return unsafe { transmute(Box::into_raw(Box::new(r))) };
                }
                _ => {
                    warn!("target type cannot be converted to: {}", to);
                    return ptr::null();
                }
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn to_redis<'a>(r: *mut KeenCacheResult<()>,
                               key: *mut c_char,
                               expire: c_int)
                               -> bool {
    let expire = expire as u64;
    with(r, |r| {
        let key = unsafe { CStr::from_ptr(key).to_str().unwrap() };
        let result = match r.type_tag {
            ResultType::POD => unsafe {
                transmute::<_, &mut KeenCacheResult<i64>>(r).to_redis(key, expire)
            },
            ResultType::DAYSITEMS => unsafe {
                transmute::<_, &mut KeenCacheResult<Days<Items>>>(r).to_redis(key, expire)
            },
            ResultType::DAYSPOD => unsafe {
                transmute::<_, &mut KeenCacheResult<Days<i64>>>(r).to_redis(key, expire)
            },
            ResultType::ITEMS => unsafe {
                transmute::<_, &mut KeenCacheResult<Items>>(r).to_redis(key, expire)
            },
        };

        if result.is_err() {
            warn!("to_redis error: {}", result.unwrap_err());
            return false;
        } else {
            return true;
        }
    })
}

#[no_mangle]
pub extern "C" fn delete_result<'a>(r: *mut KeenCacheResult<()>) {
    let ri = unsafe { Box::from_raw(r) };
    match ri.type_tag {
        ResultType::POD => {
            let r = r as *mut KeenCacheResult<i64>;
            let _ = unsafe { Box::from_raw(r) };
        }
        ResultType::DAYSITEMS => {
            let r = r as *mut KeenCacheResult<Days<Items>>;
            let _ = unsafe { Box::from_raw(r) };
        }
        ResultType::DAYSPOD => {
            let r = r as *mut KeenCacheResult<Days<i64>>;
            let _ = unsafe { Box::from_raw(r) };
        }
        ResultType::ITEMS => {
            let r = r as *mut KeenCacheResult<Items>;
            let _ = unsafe { Box::from_raw(r) };
        }
    }
    forget(ri)
}

#[no_mangle]
pub extern "C" fn result_data<'a>(r: *mut KeenCacheResult<()>) -> *const c_char {
    let r = unsafe { Box::from_raw(r) };
    info!("get data from result: found tag: {:?}", r.type_tag);

    // *KeenCacheResult destructed here
    let s: String = match r.type_tag {
        ResultType::POD => {
            let r: Box<KeenCacheResult<i64>> = unsafe { transmute(r) };
            r.to_string()
        }
        ResultType::DAYSITEMS => {
            let r: Box<KeenCacheResult<Days<Items>>> = unsafe { transmute(r) };
            r.to_string()
        }
        ResultType::DAYSPOD => {
            let r: Box<KeenCacheResult<Days<i64>>> = unsafe { transmute(r) };
            r.to_string()
        }
        ResultType::ITEMS => {
            let r: Box<KeenCacheResult<Items>> = unsafe { transmute(r) };
            r.to_string()
        }
    };

    let sr = CString::new(s).unwrap().into_raw();
    sr
}

#[no_mangle]
pub extern "C" fn from_redis<'a>(url: *const c_char,
                                 key: *const c_char,
                                 tp: c_int)
                                 -> *const KeenCacheResult<()> {
    let key = unsafe { CStr::from_ptr(key).to_str().unwrap() };
    let url = unsafe { CStr::from_ptr(url).to_str().unwrap() };
    match tp {
        POD => {
            let mut r: KeenCacheResult<i64> = match KeenCacheResult::from_redis(url, key) {
                Ok(o) => o,
                Err(e) => {
                    println!("{}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        ITEMS => {
            let mut r: KeenCacheResult<Items> = match KeenCacheResult::from_redis(url, key) {
                Ok(o) => o,
                Err(e) => {
                    println!("{}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        DAYSPOD => {
            let mut r: KeenCacheResult<Days<i64>> = match KeenCacheResult::from_redis(url,
                                                                                          key) {
                Ok(o) => o,
                Err(e) => {
                    println!("{}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        DAYSITEMS => {
            let mut r: KeenCacheResult<Days<Items>> = match KeenCacheResult::from_redis(url,
                                                                                            key) {
                Ok(o) => o,
                Err(e) => {
                    println!("{}", e);
                    return ptr::null();
                }
            };
            r.tt();
            unsafe { transmute(Box::into_raw(Box::new(r))) }
        }
        _ => {
            println!("unsupported type {}", tp);
            return ptr::null();
        }
    }
}

#[no_mangle]
pub extern "C" fn dealloc_str(s: *mut c_char) {
    unsafe { CString::from_raw(s) };
}


#[no_mangle]
pub extern "C" fn enable_log() {
    ::logger()
}
