/// Shows the different states of a fetch request
///
pub enum FetchStatus {
    /// Initially nothing to do
    None,
    /// Request is in progress
    Loading,
    /// Request is finished + data
    Finished,
}
