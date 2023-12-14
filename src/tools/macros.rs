// Copied from https://stackoverflow.com/a/27582993
// used in tests
#[allow(unused_macros)]
macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {
        std::iter::Iterator::collect(std::iter::IntoIterator::into_iter([$(($k, $v),)*]))
    };
    // set-like
    ($($v:expr),* $(,)?) => {
        std::iter::Iterator::collect(std::iter::IntoIterator::into_iter([$($v,)*]))
    };
}
