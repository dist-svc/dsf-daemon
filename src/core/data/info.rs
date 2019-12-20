use colored::*;

use dsf_core::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Queryable)]
pub struct DataInfo {
    pub service: Id,

    pub index: u16,
    pub body: Body,
    pub parent: Option<Id>,

    pub signature: Signature,
}

impl std::fmt::Display for DataInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        if f.sign_plus() {
            write!(f, "index: {}", self.index)?;
        } else {
            write!(f, "{}", self.index)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - service id: {}", self.service)?;
        } else {
            write!(f, "{}", self.service)?;
        }

        let body = match &self.body {
            Body::Cleartext(v) => format!("{:?}", v).green(),
            Body::Encrypted(_) => format!("Encrypted").red(),
            Body::None => format!("None").blue(),
        };

        if f.sign_plus() {
            write!(f, "\n  - body: {}", body)?;
        } else {
            write!(f, "{}", body)?;
        }

        let parent = match &self.parent {
            Some(p) => format!("{}", p).green(),
            None => format!("None").red(),
        };

        if f.sign_plus() {
            write!(f, "\n  - parent: {}", parent)?;
        } else {
            write!(f, "{}", parent)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - signature: {}", self.signature)?;
        } else {
            write!(f, "{}", self.signature)?;
        }

        Ok(())
    }
}