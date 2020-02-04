
use std::time::SystemTime;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::result::{Error as DieselError};
use diesel::dsl::sql_query;


use strum::{ParseError as StrumError};

use chrono::{DateTime, TimeZone, Local, NaiveDateTime};

use base64::{DecodeError as B64Error};

pub mod schema;

pub mod peers;
pub mod services;
pub mod data;

pub struct Store {
    conn: SqliteConnection,
}

fn to_dt(s: SystemTime) -> NaiveDateTime {
    DateTime::<Local>::from(s).naive_utc()
}

fn from_dt(n: &NaiveDateTime) -> SystemTime {
    let l = chrono::offset::Local;
    let dt = l.from_utc_datetime(n);
    dt.into()
}

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Diesel(DieselError),
    Strum(StrumError),
    B64(B64Error),
}

impl From<ConnectionError> for StoreError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl From<DieselError> for StoreError {
    fn from(e: DieselError) -> Self {
        Self::Diesel(e)
    }
}

impl From<StrumError> for StoreError {
    fn from(e: StrumError) -> Self {
        Self::Strum(e)
    }
}

impl From<B64Error> for StoreError {
    fn from(e: B64Error) -> Self {
        Self::B64(e)
    }
}



impl Store {
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = SqliteConnection::establish(path)?;

        Ok(Self{conn})
    }

    pub fn create(&mut self) -> Result<(), StoreError> {
        println!("Create!");

        sql_query("CREATE TABLE services (id TEXT NOT NULL UNIQUE PRIMARY KEY, 'index' INTEGER NOT NULL, state TEXT NOT NULL, public_key TEXT NOT NULL, secret_key TEXT, last_updated TEXT, subscribers INTEGER NOT NULL, replicas INTEGER NOT NULL, original BOOLEAN NOT NULL);").execute(&self.conn)?;

        sql_query("CREATE TABLE peers (id TEXT NOT NULL UNIQUE PRIMARY KEY, 'index' INTEGER, state TEXT NOT NULL, public_key TEXT, address TEXT, address_mode TEXT, last_seen TEXT, sent INTEGER NOT NULL, received INTEGER NOT NULL, blocked BOOLEAN NOT NULL);").execute(&self.conn)?;

        Ok(())
    }

    pub fn delete(&mut self) -> Result<(), StoreError> {
        sql_query("DROP TABLE IF EXISTS services;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS peers;").execute(&self.conn)?;

        Ok(())
    }
}

