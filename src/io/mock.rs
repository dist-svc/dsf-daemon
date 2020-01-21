

use super::Connector;

pub struct MockConnector {

}


#[cfg(nope)]
impl Connector for MockConnector {
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &self, req_id: RequestId, target: Address, req: NetRequest, timeout: Duration,
    ) -> Result<NetResponse, Error> {
        unimplemented!()
    }

    // Send a response message
    async fn respond(
        &self, req_id: RequestId, target: Address, resp: NetResponse,
    ) -> Result<(), Error> {
        unimplemented!()
    }
}
