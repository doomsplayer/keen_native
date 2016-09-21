error_chain! {
    // Automatic conversions between this error chain and other
    // error types not defined by the `error_chain!`. These will be
    // wrapped in a new error with, in this case, the
    // `ErrorKind::Temp` variant. The description and cause will
    // forward to the description and cause of the original error.
    //
    // This section can be empty.
    foreign_links {
        ::serde_json::error::Error, JsonError;
        ::redis::RedisError, RedisError;
        ::chrono::ParseError, ChronoError;
        ::hyper::error::Error, HyperError;
        ::protocol::KeenError, KeenError;
    }

    // Define additional `ErrorKind` variants. The syntax here is
    // the same as `quick_error!`, but the `from()` and `cause()`
    // syntax is not supported.
    errors {
        KeenioBatchError(t: String) {
            description("keenio batch error")
            display("keenio batch error: '{}'", t)
        }
    }
}
