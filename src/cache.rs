use redis::Client as RedisClient;
use redis::Connection;
use error::NativeResult;

pub fn open_redis(url: &str) -> NativeResult<Connection> {
    Ok(try!(RedisClient::open(&url[..]).and_then(|client| client.get_connection())))
}

// pub fn generate_redis_key(metric: &str, target: Option<&str>, from: &str, to: &str, interval: Option<Interval>, bound: Option<(usize, usize)>) -> String {
//     let mut s = "".to_owned();

//     s.push_str(metric);

//     target.map(|c| {
//         s.push('.');
//         s.push_str(c);
//     });

//     interval.map(|c| {
//         s.push('.');
//         s.push_str(&format!("{}", c));
//     });

//     bound.map(|c| {
//         s.push('.');
//         s.push_str(&format!("{}~{}", c.0, c.1));
//     });

//     s.push('.');
//     s.push_str(from);
//     s.push('~');
//     s.push_str(to);

//     s
// }
