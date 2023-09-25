use warp::Filter;

use crate::api::with_db;

use super::handlers::{get_domains_handler, DomainsParams};

pub fn peptide_routes(
    database_urls: Vec<String>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    get_domains(database_urls.clone())
    // .or(get_x(db.clone()))
}

fn get_domains(
    database_urls: Vec<String>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("domains"))
        .and(warp::query::<DomainsParams>())
        .and(warp::path::end())
        .and(with_db(database_urls))
        .and_then(get_domains_handler)
}
