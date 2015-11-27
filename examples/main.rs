extern crate rustc_serialize;
extern crate keenio_booster as booster;
extern crate docopt;
extern crate chrono;
extern crate env_logger;
extern crate keen;
use keen::*;
use docopt::Docopt;
use chrono::*;
use std::env;
use std::time;
use booster::*;

const USAGE: &'static str = "
keen.

Usage:
  keen <pageid>
  keen (-h | --help)

Options:
  -h --help    Show help.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    arg_pageid: usize,
}

fn main() {
    let _ = env_logger::init().unwrap();

    let key = env::var("KEEN_READ_KEY").unwrap();
    let proj = env::var("KEEN_PROJECT_ID").unwrap();
    let mut client = KeenCacheClient::new(&key, &proj);
    let r = client.set_redis("redis://127.");
    println!("{}",r.is_ok());
    client.set_timeout(time::Duration::new(30, 0));
    let metric = Metric::Count;

    // let mut q = client.query(metric.clone(), "strkingly_pageview".into(), TimeFrame::Absolute(UTC::now() - Duration::days(4), UTC::now()));
    // q.filter(Filter::eq("page_id", 458910));
    //     let d: KeenCacheResult<i64> = q.data().unwrap();
    // let s: String = d.into();
    // println!("{:?}", s);

    // let mut q = client.query(metric.clone(), "strkingly_pageview".into(), TimeFrame::Absolute(UTC::now() - Duration::days(4), UTC::now()));
    // q.filter(Filter::eq("page_id", 458910));
    // q.interval(Interval::Daily);
    // let d: KeenCacheResult<Vec<Day<i64>>> = q.data().unwrap();
    // let s: String = d.into();
    // println!("{:?}", s);

    // let mut q = client.query(metric.clone(), "strkingly_pageview".into(), TimeFrame::Absolute(UTC::now() - Duration::days(4), UTC::now()));
    // q.filter(Filter::eq("page_id", 458910));
    // q.interval(Interval::Daily);
    // let d: KeenCacheResult<Vec<Day<i64>>> = q.data().unwrap();
    // let d: KeenCacheResult<i64> = d.accumulate();
    // let s: String = d.into();
    // println!("{:?}", s);

    // let mut q = client.query(metric.clone(), "strkingly_pageview".into(), TimeFrame::Absolute(UTC::now() - Duration::days(4), UTC::now()));
    // q.filter(Filter::eq("page_id", 458910));
    // q.interval(Interval::Daily);
    // q.group_by("normalized_referrer");
    // let d: KeenCacheResult<Vec<Day<Vec<Item>>>> = q.data().unwrap();
    // let d: KeenCacheResult<i64> = d.accumulate();
    // let s: String = d.into();
    // println!("{:?}", s);

    // let mut q = client.query(metric.clone(), "strikingly_pageviews".into(), TimeFrame::Absolute(UTC::now() - Duration::days(7), UTC::now()));
    // q.filter(Filter::eq("pageId", 396));
    // q.interval(Interval::Daily);
    // q.group_by("normalized_referrer");
    // let d: KeenCacheResult<Vec<Day<Vec<Item>>>> = q.data().unwrap();
    // let d: KeenCacheResult<Vec<Day<i64>>> = d.accumulate();
    // let s: String = d.into();
    // println!("{:?}", s);

    let mut q = client.query(metric.clone(), "strikingly_pageviews".into(), TimeFrame::Absolute(UTC::now() - Duration::days(7), UTC::now()));
    q.filter(Filter::gt("pageId", 300));
    q.filter(Filter::lt("pageId", 400));
    q.interval(Interval::Daily);
    //q.group_by("normalized_referrer");
    q.group_by("pageId");
    let d: KeenCacheResult<Days<Items>> = q.data().unwrap();
    let d: KeenCacheResult<Days<i64>> = d.accumulate();
    let s: String = d.into();
    println!("{:?}", s);
}

