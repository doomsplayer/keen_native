extern crate rustc_serialize;
extern crate keenio_booster as booster;
extern crate docopt;
extern crate chrono;
extern crate env_logger;
extern crate keen;
use keen::*;
use chrono::*;
use std::env;
use std::time;
use booster::*;

fn main() {
    let _ = env_logger::init().unwrap();

    let key = env::var("KEEN_READ_KEY").unwrap();
    let proj = env::var("KEEN_PROJECT_ID").unwrap();
    let mut client = KeenCacheClient::new(&key, &proj);
    let _ = client.set_redis("redis://127.0.0.1").unwrap();
    client.set_timeout(time::Duration::new(30, 0));
    let metric = Metric::Count;

    let mut q = client.query(metric.clone(), "strikingly_pageviews".into(), TimeFrame::Absolute(UTC::now() - Duration::days(7), UTC::now()));
    q.filter(Filter::gt("pageId", 300));
    q.filter(Filter::lt("pageId", 400));
    q.interval(Interval::Daily);
    q.group_by("normalized_referrer");
    q.group_by("pageId");
    let d: KeenCacheResult<Days<Items>> = q.data().unwrap();
    let d: KeenCacheResult<Days<i64>> = d.accumulate();
    let s: String = d.to_string();
    println!("{:?}", s);
}

