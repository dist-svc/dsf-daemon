
use std::time::SystemTime;
use std::fmt;

use colored::*;

use dsf_core::prelude::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub id: Id,
    pub index: usize,
    pub state: ServiceState,
    pub public_key: PublicKey,
    pub secret_key: Option<SecretKey>,
    pub last_updated: Option<SystemTime>,
    pub subscibers: usize,
    pub replicas: usize,
    pub origin: bool,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum ServiceState {
    Created,
    Registered,
    Located,
    Subscribed,
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ServiceState::*;

        if f.sign_plus() {
            write!(f, "state: ")?;
        }

        let s = match self {
            Created =>    "created   ".yellow(),
            Registered => "registered".green(),
            Located =>    "located   ".magenta(),
            Subscribed => "subscribed".blue(),
        };

        write!(f, "{}", s)?;

        Ok(())
    }
}

impl fmt::Display for ServiceInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        if f.sign_plus() {
            write!(f, "id: {}", self.id)?;
        } else {
            write!(f, "{}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - index: {}", self.index)?;
        } else {
            write!(f, "{}", self.index)?;
        }
        
        if f.sign_plus() {
            write!(f, "\n  - state: {}", self.state)?;
        } else {
            write!(f, ", {}", self.state)?;
        }
        

        if f.sign_plus() {
            write!(f, "\n  - public key: {}", self.public_key)?;
        } else {
             write!(f, ", {}", self.public_key)?;
        }
       

        if let Some(sk) = self.secret_key {
            if f.sign_plus() {
                write!(f, "\n  - secret key: {}", sk.to_string().dimmed())?;
            } else {
                write!(f, ", {}", sk)?;
            }
            
        }

        if let Some(updated) = self.last_updated {
            let dt: chrono::DateTime<chrono::Local> = chrono::DateTime::from(updated);
            let ht = chrono_humanize::HumanTime::from(dt);

            if f.sign_plus() {
                write!(f, "\n  - last updated: {}", ht)?;
            } else {
                write!(f, ", {}", ht)?;
            }
            
        }

        if f.sign_plus() {
            write!(f, "\n  - replicas: {}", self.replicas)?;
        } else {
            write!(f, ", {}", self.replicas)?;
        }

        Ok(())
    }
}
