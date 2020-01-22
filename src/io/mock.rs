use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dsf_core::prelude::*;

use crate::error::{Error, CoreError};
use super::Connector;

#[derive(Debug, PartialEq, Clone)]
pub struct MockTransaction {
    target: Address,
    req: Option<NetRequest>,
    resp: Option<NetResponse>,
    err: Option<CoreError>,
}

impl MockTransaction {
    pub fn request(target: Address, req: NetRequest, resp: Result<NetResponse, CoreError>) -> Self {
        let (r, e) = match resp {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        };

        Self{target, req: Some(req), resp: r, err: e}
    }

    pub fn respones(target: Address, resp: NetResponse, err: Option<CoreError>) -> Self {
        Self{target, req: None, resp: Some(resp), err}
    }
}

#[derive(Clone)]
pub struct MockConnector {
    transactions: Arc<Mutex<VecDeque<MockTransaction>>>,
}

impl MockConnector {
    pub fn new() -> Self {
        Self{ transactions: Arc::new(Mutex::new(VecDeque::new())) }
    }

    /// Set expectations on the connector
    pub fn expect<T>(&mut self, transactions: T) -> Self
    where 
        T: Into<VecDeque<MockTransaction>>,
    {
        *self.transactions.lock().unwrap() = transactions.into();

        self.clone()
    }

    /// Finalise expectations on the connector
    pub fn finalise(&mut self) {
        debug!("Finalizing expectations");
        
        let transactions: Vec<_> = self.transactions.lock().unwrap().drain(..).collect();
        let expectations = Vec::<MockTransaction>::new();
        assert_eq!(
            &expectations, &transactions,
            "not all transactions have been evaluated"
        );
    }
}


#[async_trait]
impl Connector for MockConnector {
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &self, req_id: RequestId, target: Address, req: NetRequest, timeout: Duration,
    ) -> Result<NetResponse, Error> {
        
        let mut transactions = self.transactions.lock().unwrap();
        let transaction = transactions.pop_front().expect(&format!(
            "request error, no more transactions available (request: {:?})",
            req
        ));

        let request = transaction.req.as_ref().expect("expected request");

        assert_eq!(transaction.target, target, "destination mismatch");
        assert_eq!(transaction.req, Some(req), "request mismatch");

        match (transaction.resp, transaction.err) {
            (_, Some(e)) => Err(e.into()),
            (Some(r), _) => Ok(r),
            _ => unreachable!(),
        }
    }

    // Send a response message
    async fn respond(
        &self, req_id: RequestId, target: Address, resp: NetResponse,
    ) -> Result<(), Error> {
        let mut transactions = self.transactions.lock().unwrap();
        let transaction = transactions.pop_front().expect(&format!(
            "response error, no more transactions available (response: {:?})",
            resp
        ));

        let response = transaction.resp.as_ref().expect("expected response");

        assert_eq!(transaction.target, target, "destination mismatch");
        assert_eq!(transaction.resp, Some(resp), "response mismatch");

        match transaction.err {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }
}
