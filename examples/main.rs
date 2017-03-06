extern crate keenio_batch as booster;
extern crate chrono;
extern crate env_logger;
extern crate keen;
#[macro_use]
extern crate structopt_derive;
extern crate structopt;

use structopt::StructOpt;

use chrono::*;
use std::env;
use std::time;
use booster::*;

#[derive(StructOpt, Clone, Debug)]
struct Args {
    from: i64,
    to: i64,
}

fn main() {
    let args = Args::from_args();

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
    q.filter(Filter::gt("pageId", args.from));
    q.filter(Filter::lt("pageId", args.to));
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
