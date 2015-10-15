#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate rustc_serialize;
extern crate keen_native;
extern crate docopt;
extern crate chrono;
extern crate env_logger;
extern crate keen;
use keen::*;
use docopt::Docopt;
use chrono::*;

docopt!(Args, "
keen.

Usage:
  keen <pageid>
  keen (-h | --help)

Options:
  -h --help    Show help.
", arg_page_id: usize);

fn main() {
    let _ = env_logger::init().unwrap();
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let pid: usize = if let Ok(pid) = args.arg_pageid.parse() { pid } else {
        println!(r#"{{"error": "cannot parse page_id: {}"}}"#, args.arg_pageid);
        return;
    };

    let from = UTC::now() - Duration::days(1);
    let to = UTC::now();

    keen_native::cache_page_view_range(0, 1000000, from.clone(), to.clone(), true, None).unwrap();
    keen_native::cache_page_view_range(0, 1000000, from.clone(), to.clone(), false, None).unwrap();
    keen_native::cache_page_view_range(0, 1000000, from.clone(), to.clone(), true, Some(Interval::Daily)).unwrap();
    keen_native::cache_page_view_range(450000, 460000, from.clone(), to.clone(), false, Some(Interval::Hourly)).unwrap();
    keen_native::cache_with_field_range(450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), true).unwrap();
    keen_native::cache_with_field_range(450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), false).unwrap();
    keen_native::cache_with_field_range(450000, 460000, "normalized_referrer", from.clone(), to.clone(), true).unwrap();
    keen_native::cache_with_field_range(450000, 460000, "normalized_referrer", from.clone(), to.clone(), false).unwrap();

    match keen_native::get_page_view_range(pid, 0, 1000000, from.clone(), to.clone(), true, None) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_page_view_range(pid, 0, 1000000, from.clone(), to.clone(), false, None) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_page_view_range(pid, 0, 1000000, from.clone(), to.clone(), true, Some(Interval::Daily)) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_page_view_range(pid, 450000, 460000, from.clone(), to.clone(), false, Some(Interval::Hourly)) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field_range(pid, 450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), true) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field_range(pid, 450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), false) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field_range(pid, 450000, 460000, "normalized_referrer", from.clone(), to.clone(), true) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field_range(pid, 450000, 460000, "normalized_referrer", from.clone(), to.clone(), false) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

}



