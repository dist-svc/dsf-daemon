use std::{fs, io};
use std::path::{Path, PathBuf};
use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

extern crate serde_json;

/// A simple file system based key:value data store
#[derive(Clone)]
pub struct FileStore {
    dir: PathBuf,
}

#[derive(Debug)]
pub enum Error<E> {
    NotFound,
    Io(io::Error),
    Inner(E),
}

impl <E> From<io::Error> for Error<E> {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::NotFound {
            Error::NotFound
        } else {
            Error::Io(e)
        }
    }
}

impl <E> Error<E> {
    pub fn not_found(&self) -> bool {
        match self {
            Error::NotFound => true,
            _ => false,
        }
    }
}

/// EncodeDecode trait must be implemented for FileStore types
pub trait EncodeDecode {
    type Value;
    type Error;

    fn encode(value: &Self::Value) -> Result<Vec<u8>, Self::Error>;
    fn decode(buff: &[u8]) -> Result<Self::Value, Self::Error>;
}

/// Automagic EncodeDecode implementation for serde capable types
impl <V> EncodeDecode for V
where
    V: Serialize + DeserializeOwned + Debug,
{
    type Value = V;
    type Error = serde_json::Error;

    fn encode(value: &Self::Value) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(value)
    }

    fn decode(buff: &[u8]) -> Result<Self::Value, Self::Error> {
        serde_json::from_slice(&buff)
    }
}

impl FileStore 
{
    /// Create a new FileStore
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        let dir = dir.as_ref().into();

        FileStore{ dir }
    }

    pub fn create_dir(&self)-> Result<(), Error<()>> {
        fs::create_dir_all(&self.dir)?;
        Ok(())
    }

    /// List all files in the database
    pub fn list(&mut self) -> Result<Vec<String>, Error<()>> {
        let mut names = vec![];

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap();
            names.push(name);
        }

        Ok(names)
    }

    /// Load a file by name
    pub fn load<V, E, P: AsRef<Path>>(&mut self, name: P) -> Result<V, Error<E>> 
    where
        V: EncodeDecode<Value=V, Error=E> + Debug,
        E: Debug
    {
        let mut path = self.dir.clone();
        path.push(name);

        let buff = fs::read(path)?;
        let obj: V = V::decode(&buff).map_err(|e| Error::Inner(e) )?;

        Ok(obj)
    }

    /// Store a file by name
    pub fn store<V, E, P: AsRef<Path>>(&mut self, name: P, v: &V) -> Result<(), Error<E>> 
    where
        V: EncodeDecode<Value=V, Error=E> + Debug,
        E: Debug
    {
        let mut path = self.dir.clone();
        path.push(name);

        trace!("[STORE] writing file: {:?}", &path);
        
        let bin: Vec<u8> = V::encode(v).map_err(|e| Error::Inner(e) )?;
        fs::write(&path, bin)?;

        trace!("[STORE] file write done: {:?}", &path);
        Ok(())
    }

    /// Load all files from the database
    pub fn load_all<V, E>(&mut self) -> Result<Vec<(String, V)>, Error<E>> 
    where
        V: EncodeDecode<Value=V, Error=E> + Debug,
        E: Debug
    {
        let mut objs = vec![];

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap();

            let buff = fs::read(entry.path())?;
            let obj: V = V::decode(&buff).map_err(|e| Error::Inner(e) )?;

            objs.push((name, obj));
        }

        Ok(objs)
    }

    /// Store a colection of files in the database
    pub fn store_all<V, E>(&mut self, data: &[(String, V)]) -> Result<(), Error<E>> 
    where
        V: EncodeDecode<Value=V, Error=E> + Debug,
        E: Debug
    {
        for (name, value) in data {
            self.store(name, value)?;
        }

        Ok(())
    }


    /// Remove a file from the database
    pub fn rm<E, P: AsRef<Path>>(&mut self, name: P) -> Result<(), Error<E>> {
        let mut path = self.dir.clone();
        path.push(name);

        fs::remove_file(path)?;

        Ok(())
    }
}
