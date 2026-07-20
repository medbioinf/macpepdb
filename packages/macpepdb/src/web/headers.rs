use http::HeaderName;

/// The `X-Do-Not-Track` header name, checked by the tracking middleware to
/// decide whether a request should be reported to Matomo.
pub const X_DO_NOT_TRACK: HeaderName = HeaderName::from_static("x-do-not-track");
