extern crate rustc_serialize;
extern crate keen_native;
extern crate docopt;
extern crate chrono;
extern crate env_logger;
extern crate keen;
use keen::*;
use docopt::Docopt;
use chrono::*;

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
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());
    let pid: usize = args.arg_pageid;

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

//     match keen_native::get_page_view_range(pid, 0, 1000000, from.clone(), to.clone(), true, Some(Interval::Daily)) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

//     match keen_native::get_page_view_range(pid, 450000, 460000, from.clone(), to.clone(), false, Some(Interval::Hourly)) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

//     match keen_native::get_with_field_range(pid, 450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), true) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

//     match keen_native::get_with_field_range(pid, 450000, 460000, "ip_geo_info.country", from.clone(), to.clone(), false) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

//     match keen_native::get_with_field_range(pid, 450000, 460000, "normalized_referrer", from.clone(), to.clone(), true) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

//     match keen_native::get_with_field_range(pid, 450000, 460000, "normalized_referrer", from.clone(), to.clone(), false) {
//         Ok(result) => println!(r#"{{"result": {}}}"#, result),
//         Err(e) => println!(r#"{{"error": "{}"}}"#, e)
//     }

// }



