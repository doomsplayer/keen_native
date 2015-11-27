use serde::de::Error as SerdeError;
use serde_json::{Value, from_value, to_value};
use serde::{Deserialize, Deserializer, Serializer, Serialize};
use std::error::Error;
use std::fmt::Error as FmtError;
use std::fmt::Formatter;
use std::fmt::Display;
use std::collections::BTreeMap;
use serde::de::Visitor;

macro_rules! get_field {
    ($obj: expr, $field: expr) => {
        {
            let v = $obj.remove($field);
            let v = try!(v.ok_or(D::Error::missing_field($field)));
            try!(from_value(v).map_err(|e| D::Error::syntax(&format!("{:?}", e))))
        }
    }
}

pub type Items = Vec<Item>;
pub type Days<I> = Vec<Day<I>>;

// it could be
//   KeenResult<i64> for one item
//   KeenResult<Vec<Item>> for many items
//   KeenResult<Vec<Day<i64>>> same as above
//   KeenResult<Vec<Day<Vec<Item>>>> same as above
#[derive(Debug)]
pub struct KeenResult<C> {
    result: C
}

// transforms:
// Item -> POD: select 1 by attr (1 attr)
// Day<POD> -> POD: accumulate all days ()
// Day<Item> -> Item: accumulate all days ()
// Day<Item> -> Day<POD>: select 1 by attr (1 attr)
// Day<Item> -> POD: select 1 by attr (1 attr)
pub trait Accumulate<O> {
    fn accumulate(self) -> KeenResult<O>;
}
pub trait Select<O> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<O>;
}

impl Accumulate<i64> for KeenResult<Vec<Item>> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for item in &self.result {
            sum += item.result as i64;
        }
        KeenResult {
            result: sum
        }
    }
}

impl Select<i64> for KeenResult<Items> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<i64> {
        let ret = self.result.into_iter().find(|i| {
            i.fields.get(predicate.0).map(|v| v == &predicate.1).unwrap_or(false)
        }).map(|i| i.result).unwrap_or(0);
        KeenResult {
            result: ret as i64
        }
    }
}

impl Accumulate<i64> for KeenResult<Vec<Day<i64>>> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            sum += day.value as i64;
        }
        KeenResult {
            result: sum
        }
    }
}
impl Accumulate<Vec<Day<i64>>> for KeenResult<Vec<Day<Vec<Item>>>> {
    fn accumulate(self) -> KeenResult<Vec<Day<i64>>> {
        let ret = self.result.into_iter().map(|day: Day<Vec<Item>>| {
            let value: Vec<Item> = day.value;
            let mut sum: i64 = 0;
            for item in value {
                sum += item.result as i64
            }
            Day {
                value: sum,
                timeframe: day.timeframe
            }
        }).collect();
        KeenResult {
            result: ret
        }
    }
}

impl Accumulate<Vec<Item>> for KeenResult<Vec<Day<Vec<Item>>>> {
    fn accumulate(self) -> KeenResult<Vec<Item>> {
        unimplemented!()
    }
}

impl Accumulate<i64> for KeenResult<Vec<Day<Vec<Item>>>> {
    fn accumulate(self) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            for item in &day.value {
                sum += item.result as i64
            }
        }
        KeenResult {
            result: sum
        }
    }
}

impl Select<i64> for KeenResult<Days<Items>> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<i64> {
        let mut sum = 0;
        for day in &self.result {
            sum += day.value.iter().find(|i| {
                i.fields.get(predicate.0).map(|v| v == &predicate.1).unwrap_or(false)
            }).map(|i| i.result as i64).unwrap_or(0);
        }

        KeenResult {
            result: sum
        }
    }
}

impl Select<Days<Items>> for KeenResult<Days<Items>> {
    fn select(mut self, predicate: (&str, StringOrI64)) -> KeenResult<Days<Items>> {
        for day in &mut self.result {
            day.value.retain(|item| item.fields.get(predicate.0).map(|v| v == &predicate.1).unwrap_or(false));
            for item in &mut day.value {
                item.fields.remove(predicate.0);
            }
        }
        self
    }
}

impl Select<Days<i64>> for KeenResult<Days<Items>> {
    fn select(self, predicate: (&str, StringOrI64)) -> KeenResult<Days<i64>> {
        KeenResult {
            result: self.result.into_iter().map(|day| {
                let v = day.value.iter().find(|i| {
                    i.fields.get(predicate.0).map(|v| v == &predicate.1).unwrap_or(false)
                }).map(|i| i.result as i64).unwrap_or(0);
                Day {
                    value: v,
                    timeframe: day.timeframe
                }
            }).collect()
        }
    }
}

impl<C> Deserialize for KeenResult<C> where C: Deserialize {
    fn deserialize<D>(deserializer: &mut D) -> Result<KeenResult<C>, D::Error> where D: Deserializer {
        let mut bt: BTreeMap<String, C> = try!(BTreeMap::deserialize(deserializer));
        if let Some(result) = bt.remove("result") {
            Ok(KeenResult {
                result: result
            })
        } else {
            Err(D::Error::missing_field("result"))
        }
    }
}

impl<C> Serialize for KeenResult<C> where C: Serialize {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(vec![("result", &self.result)].into_iter(), Some(1)))
    }
}

#[derive(Debug)]
pub struct Day<V> {
    value: V,
    timeframe: Timeframe
}

impl<V> Deserialize for Day<V> where V: Deserialize {
    fn deserialize<D>(deserializer: &mut D) -> Result<Day<V>, D::Error> where D: Deserializer {
        let mut object: BTreeMap<String, Value> = try!(Deserialize::deserialize(deserializer));
        let value = get_field!(object, "value");
        let timeframe = get_field!(object, "timeframe");

        Ok(Day {
            value: value,
            timeframe: timeframe
        })
    }
}

impl<V> Serialize for Day<V> where V: Serialize {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(
            vec![("value", to_value(&self.value)),
                ("timeframe", to_value(&self.timeframe))].into_iter(), Some(2)))
    }
}

#[derive(Debug,Clone)]
pub enum StringOrI64 {
    String(String),
    I64(i64)
}

impl PartialEq for StringOrI64 {
    fn eq(&self, other: &StringOrI64) -> bool {
        match (self, other) {
            (&StringOrI64::I64(i), &StringOrI64::I64(j)) => {
                i == j
            }
            (&StringOrI64::I64(i), &StringOrI64::String(ref j)) => {
                format!("{}", i) == &j[..]
            }
            (&StringOrI64::String(ref i), &StringOrI64::I64(j)) => {
                &i[..] == format!("{}", j)
            }
            (&StringOrI64::String(ref i), &StringOrI64::String(ref j)) => {
                i == j
            }
        }
    }
}
struct StringOrI64Visitor;
impl Visitor for StringOrI64Visitor {
    type Value = StringOrI64;
    fn visit_i8<E>(&mut self, value: i8) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_i16<E>(&mut self, value: i16) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_i32<E>(&mut self, value: i32) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_i64<E>(&mut self, value: i64) -> Result<StringOrI64, E> where E: SerdeError {
        Ok(StringOrI64::I64(value))
    }
    fn visit_isize<E>(&mut self, value: isize) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_u8<E>(&mut self, value: u8) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_u16<E>(&mut self, value: u16) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_u32<E>(&mut self, value: u32) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_u64<E>(&mut self, value: u64) -> Result<StringOrI64, E> where E: SerdeError {
        Ok(StringOrI64::I64(value as i64))
    }
    fn visit_usize<E>(&mut self, value: usize) -> Result<StringOrI64, E> where E: SerdeError {
        self.visit_i64(value as i64)
    }
    fn visit_str<E>(&mut self, value: &str) -> Result<StringOrI64, E> where E: SerdeError {
        Ok(StringOrI64::String(value.into()))
    }
    fn visit_string<E>(&mut self, value: String) -> Result<StringOrI64, E> where E: SerdeError {
        Ok(StringOrI64::String(value))
    }
}

impl Deserialize for StringOrI64 {
    fn deserialize<D>(deserializer: &mut D) -> Result<StringOrI64, D::Error> where D: Deserializer {
        let value: StringOrI64 = try!(deserializer.visit(StringOrI64Visitor));
        Ok(value)
    }
}

impl Serialize for StringOrI64 {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        match self {
            &StringOrI64::String(ref s) => serializer.visit_str(s),
            &StringOrI64::I64(i) => serializer.visit_i64(i)
        }
    }
}

#[derive(Debug)]
pub struct Item {
    result: u64,
    fields: BTreeMap<String, StringOrI64>,
}

impl Deserialize for Item {
    fn deserialize<D>(deserializer: &mut D) -> Result<Item, D::Error> where D: Deserializer {
        let mut object: BTreeMap<String, Value> = try!(Deserialize::deserialize(deserializer));
        let result = try!(object.remove("result").and_then(|v| v.as_u64()).ok_or(D::Error::missing_field("no such field: result")));

        let object = object.into_iter().map(|(k,v)| {
            let v = if v.is_number() {
                StringOrI64::I64(v.as_i64().unwrap())
            } else if v.is_string() {
                StringOrI64::String(v.as_string().unwrap().into())
            } else {
                StringOrI64::I64(0)
            };
            (k, v)
        }).collect();
        let page = Item {
            result: result,
            fields: object
        };
        Ok(page)
    }
}

impl Serialize for Item {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut bt = self.fields.clone();
        bt.insert("result".to_owned(), StringOrI64::I64(self.result as i64));
        bt.serialize(serializer)
    }
}

#[derive(Debug)]
struct Timeframe {
    start: String,
    end: String
}

impl Deserialize for Timeframe {
    fn deserialize<D>(deserializer: &mut D) -> Result<Timeframe, D::Error> where D: Deserializer {
        
        let mut object: BTreeMap<String, String> = try!(BTreeMap::deserialize(deserializer));
        let start = try!(object.remove("start").ok_or(D::Error::missing_field("no such field: start")));
        let end = try!(object.remove("end").ok_or(D::Error::missing_field("no such field: end")));

        Ok(Timeframe {
            start: start,
            end: end
        })
    }
}

impl Serialize for Timeframe {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        use serde::ser::impls::MapIteratorVisitor;
        serializer.visit_map(MapIteratorVisitor::new(
            vec![("start", self.start.clone()), ("end", self.end.clone())].into_iter(), Some(2)))
    }
}

#[derive(Debug)]
pub struct KeenError {
    message: String,
    error_code: String
}

impl Deserialize for KeenError {
    fn deserialize<D>(deserializer: &mut D) -> Result<KeenError, D::Error> where D: Deserializer {
        let mut object: BTreeMap<String, String> = try!(BTreeMap::deserialize(deserializer));
        Ok(KeenError {
            message: try!(object.remove("message").ok_or(D::Error::missing_field("no such field: message"))),
            error_code: try!(object.remove("error_code").ok_or(D::Error::missing_field("no such field: error_code")))
        })
    }
}

impl Display for KeenError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        f.write_str(&format!("{}: {}", self.error_code, self.message))
    }
}

impl Error for KeenError {
    fn description(&self) -> &str {
        &self.message
    }
}
