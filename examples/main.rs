extern crate rustc_serialize;
extern crate keenio_batch as booster;
extern crate chrono;
extern crate env_logger;
extern crate keen;
extern crate docopt;

use chrono::*;
use std::env;
use std::time;
use booster::*;

static USAGE: &'static str = "
Usage:
  main <from> <to>
";

#[derive(Debug, RustcDecodable)]
struct Args {
    arg_from: i64,
    arg_to: i64,
}

fn main() {
    let args: Args = docopt::Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());


    let _ = env_logger::init().unwrap();

    let key = env::var("KEEN_READ_KEY").unwrap();
    let proj = env::var("KEEN_PROJECT_ID").unwrap();

    let mut client = KeenCacheClient::new(&key, &proj);
    let _ = client.set_redis("redis://127.0.0.1").unwrap();

    client.set_timeout(time::Duration::new(30, 0));
    let metric = Metric::CountUnique("ip_address".into());

    let mut q = client.query(metric.clone(),
                             "strikingly_pageviews".into(),
                             TimeFrame::Absolute(UTC::now() - Duration::hours(24),
                                                 UTC::now() - Duration::hours(1)));
    q.filter(Filter::gt("pageId", args.arg_from));
    q.filter(Filter::lt("pageId", args.arg_to));
    q.group_by("normalized_referrer");
    q.group_by("ip_geo_info.country");
    q.group_by("parsed_user_agent.os.family");
    q.group_by("pageId");
    match q.data::<Items>() {
        Ok(d) => {
            let s: String = d.to_string();
            println!("{:?}", s);
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
}
