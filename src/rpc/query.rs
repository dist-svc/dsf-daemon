




use rr_mux::Connector;



use dsf_core::prelude::*;

use dsf_core::net;


use crate::core::ctx::Ctx;

use crate::daemon::dsf::Dsf;


impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{


}