
use std::time::SystemTime;

use dsf_core::prelude::*;

use super::peers::Peer;

#[derive(Debug, Clone)]
pub struct Replica {
    pub(crate) id: Id,
    pub(crate) version: u16,

    pub(crate) peer: Option<Peer>,
    
    pub(crate) issued: SystemTime,
    pub(crate) expiry: SystemTime,

    pub(crate) active: bool,
}

use std::fmt;

impl fmt::Display for Replica {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        if f.sign_plus() {
            write!(f, "service id: {}", self.id)?;
        } else {
            write!(f, "{}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, " - version: {}", self.version)?;
        } else {
            write!(f, "{}", self.version)?;
        }

        // TODO: the rest of this

        Ok(())
    }
}