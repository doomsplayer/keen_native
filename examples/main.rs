#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate rustc_serialize;
extern crate keen_native;
extern crate docopt;
extern crate chrono;
extern crate env_logger;
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
    env_logger::init();
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let pid: usize = if let Ok(pid) = args.arg_pageid.parse() { pid } else {
        println!(r#"{{"error": "cannot parse page_id: {}"}}"#, args.arg_pageid);
        return;
    };

    let from = UTC::now() - Duration::days(1);
    let to = UTC::now();

    keen_native::cache_total_unique_page_view(from.clone(), to.clone()).unwrap();
    keen_native::cache_total_page_view(from.clone(), to.clone()).unwrap();
    keen_native::cache_unique_page_view(450000, 460000, from.clone(), to.clone()).unwrap();
    keen_native::cache_with_field(450000, 460000, "ip_geo_info.country", from.clone(), to.clone()).unwrap();
    keen_native::cache_with_field(450000, 460000, "normalized_referrer", from.clone(), to.clone()).unwrap();

    match keen_native::get_total_unique_page_view(pid, from.clone(), to.clone()) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_total_page_view(pid, from.clone(), to.clone()) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_unique_page_view(pid, 450000, 460000, from.clone(), to.clone()) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field(pid, 450000, 460000, "ip_geo_info.country", from.clone(), to.clone()) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }

    match keen_native::get_with_field(pid, 450000, 460000, "normalized_referrer", from.clone(), to.clone()) {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }
}


