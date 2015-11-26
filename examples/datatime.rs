extern crate chrono;
use chrono::*;

fn main() {
    let t: Result<DateTime<UTC>,_> = "2015-11-15T01:22:00-08:00".parse();
    println!("{:?}",t);
}
