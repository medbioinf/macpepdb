// std imports
use std::sync::Arc;

use axum::Router;
// 3rd party imports
use axum::extract::{Json, State};
use axum::routing::get;

// internal imports
use crate::configuration::RuntimeConfiguration;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/configuration";
static SHOW_PATH: &str = "/";

pub struct ConfigurationController;

impl ConfigurationController {
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new().route(SHOW_PATH, get(Self::show));

        router.with_state(state)
    }

    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Returns the configuration with which MaCPepDB was created.
    ///
    /// # Arguments
    /// * `state` - The state of the server.
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/configuration`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "mass_partitioning": {
    ///         "single": [
    ///             [mass_1, partition_1],
    ///             ...
    ///         ],
    ///         "overflow": {
    ///             "mass_n-1": {
    ///                 "partition_n-1": null,
    ///                 ...
    ///             },
    ///             ...
    ///         }
    ///     },
    ///     protease": {
    ///         "inner":"trypsin",
    ///         "semi_specific":false,
    ///         "min_length":6,
    ///         "max_length":50,
    ///         "max_missed_cleavages":2,
    ///         "keep_unknown":false
    ///     }
    /// }
    /// ```
    ///
    pub async fn show(State(server_state): State<Arc<ServerState>>) -> Json<RuntimeConfiguration> {
        Json(server_state.configuration_as_ref().clone())
    }
}
