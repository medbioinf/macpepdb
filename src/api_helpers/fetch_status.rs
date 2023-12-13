/// Shows the different states of a fetch request
///
/// # Generic parameters
/// * `T`: Type of the data that is fetched
///
pub enum FetchStatus<T> {
    /// Initially nothing to do
    None,
    /// Request is in progress
    Loading,
    /// Request is finished + data
    Finished(T),
}
