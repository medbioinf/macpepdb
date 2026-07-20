macro_rules! into_thiserror_boxed {
    ($from_error:ty, $thiserror:ty, $thiserror_variant:ident) => {
        impl From<$from_error> for $thiserror {
            fn from(value: $from_error) -> Self {
                <$thiserror>::$thiserror_variant(Box::new(value))
            }
        }
    };
}
