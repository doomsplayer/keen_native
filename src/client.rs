use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{from_reader, from_str, to_string};

use chrono::{DateTime, UTC};
use hyper::status::StatusCode;
use redis::{Commands, Connection, Client as RedisClient};
use keen::{Filter, Interval, KeenClient, KeenQuery, Metric, TimeFrame};

use protocol::{Accumulate, Days, KeenError, KeenResult, Range, Select, StringOrI64};
use errors::Result;

macro_rules! timeit {
    ($e: expr, $f: expr, $t: expr) => {
        {
            let t = UTC::now();
            let result = $e;
            if $t { info!("keen native: {} :{}", $f, UTC::now().signed_duration_since(t)) }
            result
        }
    };
    ($e: expr, $f: expr) => {
        {
            let t = UTC::now();
            let result = $e;
            info!("{} :{}", $f, UTC::now().signed_duration_since(t));
            result
        }
    };
}

pub struct KeenCacheClient {
    client: KeenClient,
    redis: Option<RedisClient>,
}

impl KeenCacheClient {
    pub fn new(key: &str, project: &str) -> KeenCacheClient {
        let _ = ::env_logger::init();
        KeenCacheClient {
            client: KeenClient::new(key, project),
            redis: None,
        }
    }
    pub fn set_redis(&mut self, url: &str) -> Result<()> {
        let client = try!(open_redis(url));
        self.redis = Some(client);
        Ok(())
    }
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.client.timeout(timeout);
    }
    pub fn query(&self,
                 metric: Metric,
                 collection: String,
                 timeframe: TimeFrame)
                 -> KeenCacheQuery {
        KeenCacheQuery {
            query: self.client.query(metric, collection, timeframe),
            redis: self.redis.clone(),
            tp: ResultType::POD,
        }
    }
}

#[derive(Clone, Copy)]
pub enum ResultType {
    POD,
    Items,
    DaysPOD,
    DaysItems,
}

pub struct KeenCacheQuery {
    query: KeenQuery,
    redis: Option<RedisClient>,
    pub tp: ResultType,
}

impl KeenCacheQuery {
    pub fn group_by(&mut self, g: &str) {
        use self::ResultType::*;

        self.query.group_by(g);
        self.tp = match self.tp {
            POD => Items,
            DaysPOD => DaysItems,
            o => o,
        }
    }
    pub fn filter(&mut self, f: Filter) {
        self.query.filter(f);
    }
    pub fn interval(&mut self, i: Interval) {
        use self::ResultType::*;

        self.query.interval(i);
        self.tp = match self.tp {
            POD => DaysPOD,
            Items => DaysItems,
            o => o,
        }
    }
    pub fn max_age(&mut self, age: usize) {
        self.query.max_age(age);
    }
    pub fn other(&mut self, key: &str, value: &str) {
        self.query.other(key, value);
    }
    pub fn data<C>(&self) -> Result<KeenCacheResult<C>>
        where C: Deserialize
    {
        debug!("get data from keenio: url is : {}", self.query.url());

        let resp = try!(timeit!(self.query.data(), "get data from keen io"));

        debug!("response from keenio's url is: {}", resp.url);

        if resp.status != StatusCode::Ok {
            let e: KeenError = try!(from_reader(resp));
            return Err(e.into());
        }

        let connection = if let Some(ref client) = self.redis {
            Some(try!(client.get_connection()))
        } else {
            None
        };

        let ret = KeenCacheResult {
            data: try!(timeit!(from_reader(resp), "decode data from reader")),
            redis: connection,
        };
        Ok(ret)
    }
}

pub struct KeenCacheResult<C> {
    data: KeenResult<C>,
    redis: Option<Connection>,
}

impl<C> KeenCacheResult<C>
    where C: Deserialize
{
    pub fn from_str(payload: &str) -> Result<KeenCacheResult<C>> {
        let result = timeit!(from_str(&payload), "decode data from redis")?;
        Ok(KeenCacheResult {
            data: result,
            redis: None,
        })
    }

    pub fn from_redis(url: &str, key: &str) -> Result<KeenCacheResult<C>> {
        let c = open_redis(url)?;
        let s: String = timeit!(c.get(key), "get data from redis")?;
        let result = timeit!(from_str(&s), "decode data from redis")?;
        Ok(KeenCacheResult {
            data: result,
            redis: None,
        })
    }
}

impl<C> KeenCacheResult<Days<C>> {
    pub fn range(self, from: DateTime<UTC>, to: DateTime<UTC>) -> KeenCacheResult<Days<C>> {
        let r = KeenCacheResult {
            data: self.data.range(from, to),
            redis: self.redis,
        };
        r
    }
}
impl<C> KeenCacheResult<C>
    where C: Serialize
{
    pub fn accumulate<O>(self) -> KeenCacheResult<O>
        where KeenResult<C>: Accumulate<O>
    {
        let r = KeenCacheResult {
            data: self.data.accumulate(),
            redis: self.redis,
        };
        r
    }
    pub fn select<O, I>(self, predicate: (&str, I)) -> KeenCacheResult<O>
        where KeenResult<C>: Select<O>,
              I: Into<StringOrI64>
    {
        let r = KeenCacheResult {
            data: self.data.select(predicate),
            redis: self.redis,
        };
        r
    }
    pub fn to_redis(&self, key: &str, expire: u64) -> Result<()> {
        let bin = try!(to_string(&self.data));
        if self.redis.is_some() {
            let _: () = self.redis.as_ref().unwrap().set(&key[..], bin)?;
            let _: () = self.redis.as_ref().unwrap().expire(&key[..], expire as usize)?;
        }
        Ok(())
    }
    pub fn to_string(&self) -> String {
        to_string(&self.data).unwrap()
    }
}

fn open_redis(url: &str) -> Result<RedisClient> {
    Ok(try!(RedisClient::open(&url[..])))
}