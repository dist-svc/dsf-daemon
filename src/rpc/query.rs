use crate::daemon::{Dsf, net::NetIf};

impl <Net> Dsf<Net> where Dsf<Net>: NetIf<Interface=Net> {

}
