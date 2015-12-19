use std::error::Error;
use std::fmt::Error as FmtError;
use std::fmt::Formatter;
use std::fmt::Display;
use protocol::KeenError;
use chrono::ParseError;
use hyper::error::Error as HyperError;
use serde_json::error::Error as JsonError;
use redis::RedisError;
use std::env;
use std::borrow::Cow;
use std::borrow::Borrow;

wrapped_enum! {
    #[derive(Debug)]
    /// Error type of Keen Native library
    pub enum NativeError {
        /// native error for this library
        NativeError(Cow<'static, str>),
        /// json error
        JsonError(JsonError),
        /// env error
        EnvError(env::VarError),
        /// redis error
        RedisError(RedisError),
        /// hyper error
        HttpError(HyperError),
        /// chrono error
        TimeError(ParseError),
        /// keen error
        KeenError(KeenError)
    }
}

impl Display for NativeError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        f.write_str(&format!("{:?}", self))
    }
}

impl Error for NativeError {
    fn description(&self) -> &str {
        use self::NativeError::*;

        match self {
            &NativeError(ref s) => s.borrow(),
            &JsonError(ref s) => s.description(),
            &EnvError(ref s) => s.description(),
            &HttpError(ref s) => s.description(),
            &RedisError(ref s) => s.description(),
            &TimeError(ref s) => s.description(),
            &KeenError(ref s) => s.description(),
        }
    }
}

pub type NativeResult<T> = Result<T, NativeError>;
