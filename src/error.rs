use std::io;
use failure::Fail;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "bincode error: {}", _0)]
    Serde(#[cause] bincode::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::Serde(err)
    }
}
