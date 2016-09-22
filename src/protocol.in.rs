use serde::de::Error as SerdeError;
#[allow(unused_imports)]
use serde_json::{from_str, to_string, to_value, Value};
use serde::{Deserialize, Deserializer, Serializer, Serialize};
use std::error::Error;
use std::fmt::Error as FmtError;
use std::fmt::Formatter;
use std::fmt::Display;
use std::collections::BTreeMap;
use serde::de::Visitor;
use std::ops::{Deref, DerefMut};
use chrono::DateTime;
use chrono::UTC;

pub type Days<I> = Vec<Day<I>>;

// it could be
//   KeenResult<i64> for one item
//   KeenResult<Vec<Item>> for many items
//   KeenResult<Vec<Day<i64>>> same as above
//   KeenResult<Vec<Day<Vec<Item>>>> same as above
#[derive(Debug, Deserialize, Serialize)]
pub struct KeenResult<C> {
    result: C,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Day<V> {
    value: V,
    timeframe: Timeframe,
}

#[derive(Debug,Clone)]
pub enum StringOrI64 {
    String(String),
    I64(i64),
}

impl PartialEq for StringOrI64 {
    fn eq(&self, other: &StringOrI64) -> bool {
        match (self, other) {
            (&StringOrI64::I64(i), &StringOrI64::I64(j)) => i == j,
            (&StringOrI64::I64(i), &StringOrI64::String(ref j)) => format!("{}", i) == &j[..],
            (&StringOrI64::String(ref i), &StringOrI64::I64(j)) => &i[..] == format!("{}", j),
            (&StringOrI64::String(ref i), &StringOrI64::String(ref j)) => i == j,
        }
    }
}



impl Deserialize for StringOrI64 {
    fn deserialize<D>(deserializer: &mut D) -> Result<StringOrI64, D::Error>
        where D: Deserializer
    {
        struct StringOrI64Visitor;
        impl Visitor for StringOrI64Visitor {
            type Value = StringOrI64;
            fn visit_i8<E>(&mut self, value: i8) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_i16<E>(&mut self, value: i16) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_i32<E>(&mut self, value: i32) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_i64<E>(&mut self, value: i64) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                Ok(StringOrI64::I64(value))
            }
            fn visit_isize<E>(&mut self, value: isize) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_u8<E>(&mut self, value: u8) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_u16<E>(&mut self, value: u16) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_u32<E>(&mut self, value: u32) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_u64<E>(&mut self, value: u64) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                Ok(StringOrI64::I64(value as i64))
            }
            fn visit_usize<E>(&mut self, value: usize) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                self.visit_i64(value as i64)
            }
            fn visit_str<E>(&mut self, value: &str) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                Ok(StringOrI64::String(value.into()))
            }
            fn visit_string<E>(&mut self, value: String) -> Result<StringOrI64, E>
                where E: SerdeError
            {
                Ok(StringOrI64::String(value))
            }
        }
        let value: StringOrI64 = try!(deserializer.deserialize(StringOrI64Visitor));
        Ok(value)
    }
}

impl Serialize for StringOrI64 {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: Serializer
    {
        match self {
            &StringOrI64::String(ref s) => serializer.serialize_str(s),
            &StringOrI64::I64(i) => serializer.serialize_i64(i),
        }
    }
}

#[derive(Debug)]
pub struct Items(Vec<Item>);

impl Deref for Items {
    type Target = Vec<Item>;
    fn deref(&self) -> &Vec<Item> {
        &self.0
    }
}

impl DerefMut for Items {
    fn deref_mut(&mut self) -> &mut Vec<Item> {
        &mut self.0
    }
}

impl Deserialize for Items {
    fn deserialize<D>(deserializer: &mut D) -> Result<Items, D::Error>
        where D: Deserializer
    {
        let mut v = try!(Vec::<Item>::deserialize(deserializer));
        v.retain(|i| i.result != 0);
        Ok(Items(v))
    }
}

impl Serialize for Items {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: Serializer
    {
        self.0.serialize(serializer)
    }
}

#[derive(Debug)]
pub struct CompressedFields(String);

impl CompressedFields {
    pub fn get(&self, key: &str) -> Option<StringOrI64> {
        let g: Result<BTreeMap<String, StringOrI64>, _> = from_str(&self.0);
        g.ok().and_then(|bt| bt.get(key).map(|s| s.clone()))
    }
    pub fn remove(&mut self, key: &str) {
        let g: Result<BTreeMap<String, StringOrI64>, _> = from_str(&self.0);
        self.0 = g.ok()
            .and_then(|mut bt| {
                bt.remove(key);
                to_string(&bt).ok()
            })
            .unwrap_or_default();
    }
}

#[derive(Debug)]
pub struct Item {
    result: u64,
    fields: CompressedFields,
}

// BTreeMap<String, StringOrI64>
impl Deserialize for Item {
    fn deserialize<D>(deserializer: &mut D) -> Result<Item, D::Error>
        where D: Deserializer
    {
        use serde_json::ser::to_string;
        let mut object: BTreeMap<String, Value> = try!(Deserialize::deserialize(deserializer));
        let result = try!(object.remove("result")
            .and_then(|v| v.as_u64())
            .ok_or(D::Error::missing_field("no such field: result")));

        let fields = to_string(&object).unwrap();

        let page = Item {
            result: result,
            fields: CompressedFields(fields),
        };
        Ok(page)
    }
}

impl Serialize for Item {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: Serializer
    {
        use serde_json::from_str;
        let mut object: BTreeMap<String, Value> = from_str(&self.fields.0).ok().unwrap_or_default();
        object.insert("result".to_owned(), Value::I64(self.result as i64));
        object.serialize(serializer)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Timeframe {
    start: String,
    end: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KeenError {
    message: String,
    error_code: String,
}

impl Display for KeenError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{}: {}", self.error_code, self.message)
    }
}

impl Error for KeenError {
    fn description(&self) -> &str {
        &self.message
    }
}

// transforms:
// transforms:
// Item -> POD: select 1 by attr (1 attr)
// Day<POD> -> POD: accumulate all days ()
// Day<Item> -> Item: accumulate all days ()
// Day<Item> -> Day<POD>: select 1 by attr (1 attr)
// Day<Item> -> POD: select 1 by attr (1 attr)
pub trait Accumulate<O> {
    fn accumulate(self) -> KeenResult<O>;
}

impl Accumulate<i64> for KeenResult<Items> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for item in &self.result.0 {
            sum += item.result as i64;
        }
        KeenResult { result: sum }
    }
}

impl Accumulate<i64> for KeenResult<Days<i64>> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            sum += day.value as i64;
        }
        KeenResult { result: sum }
    }
}
impl Accumulate<Days<i64>> for KeenResult<Days<Items>> {
    fn accumulate(self) -> KeenResult<Days<i64>> {
        let ret = self.result
            .into_iter()
            .map(|day: Day<Items>| {
                let value: Items = day.value;
                let mut sum: i64 = 0;
                for item in value.0 {
                    sum += item.result as i64
                }
                Day {
                    value: sum,
                    timeframe: day.timeframe,
                }
            })
            .collect();
        KeenResult { result: ret }
    }
}

impl Accumulate<Items> for KeenResult<Days<Items>> {
    fn accumulate(self) -> KeenResult<Items> {
        unimplemented!()
    }
}

impl Accumulate<i64> for KeenResult<Days<Items>> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            for item in &day.value.0 {
                sum += item.result as i64
            }
        }
        KeenResult { result: sum }
    }
}

pub trait Select<O> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<O>;
}

impl Select<i64> for KeenResult<Items> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<i64> {
        let ret = self.result
            .0
            .into_iter()
            .find(|i| i.fields.get(predicate.0).map(|v| v == predicate.1).unwrap_or(false))
            .map(|i| i.result)
            .unwrap_or(0);
        KeenResult { result: ret as i64 }
    }
}

impl Select<Items> for KeenResult<Items> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<Items> {
        let ret: Vec<_> = self.result
            .0
            .into_iter()
            .filter(|i| i.fields.get(predicate.0).map(|v| v == predicate.1).unwrap_or(false))
            .collect();
        KeenResult { result: Items(ret) }
    }
}

impl Select<i64> for KeenResult<Days<Items>> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            sum += day.value
                .iter()
                .find(|i| i.fields.get(predicate.0).map(|v| v == predicate.1).unwrap_or(false))
                .map(|i| i.result as i64)
                .unwrap_or(0);
        }
        KeenResult { result: sum }
    }
}

impl Select<Days<Items>> for KeenResult<Days<Items>> {
    fn select(mut self, predicate: (&str, StringOrI64)) -> KeenResult<Days<Items>> {
        for day in &mut self.result {
            day.value.retain(|item| {
                item.fields.get(predicate.0).map(|v| v == predicate.1).unwrap_or(false)
            });
            for item in &mut day.value.0 {
                item.fields.remove(predicate.0);
            }
        }
        self
    }
}

impl Select<Days<i64>> for KeenResult<Days<Items>> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<Days<i64>> {
        KeenResult {
            result: self.result
                .into_iter()
                .map(|day| {
                    let v = day.value
                        .iter()
                        .find(|i| {
                            i.fields.get(predicate.0).map(|v| v == predicate.1).unwrap_or(false)
                        })
                        .map(|i| i.result as i64)
                        .unwrap_or(0);
                    Day {
                        value: v,
                        timeframe: day.timeframe,
                    }
                })
                .collect(),
        }
    }
}

pub trait Range<O> {
    fn range(self, from: DateTime<UTC>, to: DateTime<UTC>) -> KeenResult<O>;
}

impl<C> Range<Days<C>> for KeenResult<Days<C>> {
    fn range(mut self, from: DateTime<UTC>, to: DateTime<UTC>) -> KeenResult<Days<C>> {
        self.result.retain(|d| {
            from <= d.timeframe.start.parse().ok().unwrap_or(UTC::now()) &&
            d.timeframe.end.parse().ok().unwrap_or(UTC::now()) <= to
        });
        self
    }
}

pub trait Merge<O> {
    fn merge(self, rhs: KeenResult<O>) -> KeenResult<O>;
}

impl Merge<Days<i64>> for KeenResult<Days<i64>> {
    fn merge(mut self, mut rhs: KeenResult<Days<i64>>) -> KeenResult<Days<i64>> {
        self.result.append(&mut rhs.result);
        self.result.sort_by(|a, b| a.timeframe.start.cmp(&b.timeframe.start));
        self
    }
}
