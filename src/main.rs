#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate rustc_serialize;
extern crate keen;
extern crate docopt;
extern crate chrono;
use docopt::Docopt;
use chrono::*;

docopt!(Args, "
keen.

Usage:
  keen <url> <pageid> <fromdate> [-r <redis>] [-d] [-a]
  keen (-h | --help)

Options:
  -d           Debug.
  -a           Aggregate.
  -r           Redis connection.
  -h --help    Show help.
", arg_page_id: usize);

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let pid: usize = if let Ok(pid) = args.arg_pageid.parse() { pid } else {
        println!(r#"{{"error": "cannot parse page_id: {}"}}"#, args.arg_pageid);
        return;
    };

    let time: DateTime<UTC> = match args.arg_fromdate.parse() {
        Ok(time) => time,
        Err(e) => {
            println!(r#"{{"error": "cannot parse datetime: {}"}}"#, e);
            return;
        }
    };

    let mut options = keen::KeenOptions::new(&args.arg_url, pid, time);
    options.set_debug(args.flag_d);
    options.set_aggregate(args.flag_a);
    if args.flag_r {
        options.set_redis(&args.arg_redis);
    }

    match options.get_data() {
        Ok(result) => println!(r#"{{"result": {}}}"#, result),
        Err(e) => println!(r#"{{"error": "{}"}}"#, e)
    }
}


