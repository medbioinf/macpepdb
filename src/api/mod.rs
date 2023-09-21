use std::convert::Infallible;

use warp::Filter;

use crate::database::scylla::{client::Client, get_client};

pub mod peptides;

fn with_db(
    database_urls: Vec<String>,
) -> impl Filter<Extract = (Vec<String>,), Error = Infallible> + Clone {
    warp::any().map(move || database_urls.clone())
}
