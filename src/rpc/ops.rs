

use futures::channel::mpsc;

use dsf_rpc::StatusInfo;
use super::connect::ConnectCtx;


use crate::error::Error;

/// RPC operation container object
/// Used to track RPC operation kind / state / response etc.
pub struct RpcOperation {
    pub req_id: u64,
    pub state: RpcState,
    pub kind: RpcKind,
}



/// RPC operation kind enmeration
pub enum RpcKind {
    Status(mpsc::Sender<StatusInfo>),
    Connect(ConnectCtx),
}

impl std::fmt::Display for RpcKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcKind::Status(_) => write!(f, "Status"),
            RpcKind::Connect(_) => write!(f, "Connect"),
        }
    }
}


pub enum RpcState {
    /// Initialised state, nothing done yet
    Init,

}

