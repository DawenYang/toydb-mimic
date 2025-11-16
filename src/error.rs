use std::fmt::Display;

use serde::{
    Deserialize, Serialize,
    de::{DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Abort,
    InvalidData(String),
    InvalidInput(String),
    IO(String),
    ReadOnly,
    Serialization,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "operation aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
            Error::ReadOnly => write!(f, "read-only transaction"),
            Error::Serialization => write!(f, "serialization failure, retry transaction"),
        }
    }
}

impl Error {
    /// Returns whether the error is considered deterministic. Raft state
    /// machine application needs to know whether a command failure is
    /// deterministic on the input command -- if it is, the command can be
    /// considered applied and the error returned to the client, but otherwise
    /// the state machine must panic to prevent node divergence.
    pub fn is_deterministic(&self) -> bool {
        match self {
            // Aborts don't happen during application, only leader changes. But
            // we consider them non-deterministic in case an abort should happen
            // unexpectedly below Raft.
            Error::Abort => false,
            // Possible data corruption local to this node.
            Error::InvalidData(_) => false,
            // Input errors are (likely) deterministic. They might not be in
            // case data was corrupted in flight, but we ignore this case.
            Error::InvalidInput(_) => true,
            // IO errors are typically local to the node (e.g. faulty disk).
            Error::IO(_) => false,
            // Write commands in read-only transactions are deterministic.
            Error::ReadOnly => true,
            // Write conflicts are determinstic.
            Error::Serialization => true,
        }
    }
}

/// Constructs an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
}

/// Constructs an Error::InvalidInput for the given format string.
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
}

/// A toyDB Result returning Error.
pub type Result<T> = std::result::Result<T, Error>;

impl<T> From<Error> for Result<T> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<config::ConfigError> for Error {
    fn from(err: config::ConfigError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<crossbeam::channel::RecvError> for Error {
    fn from(err: crossbeam::channel::RecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for Error {
    fn from(err: crossbeam::channel::SendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<crossbeam::channel::TryRecvError> for Error {
    fn from(err: crossbeam::channel::TryRecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::TrySendError<T>> for Error {
    fn from(err: crossbeam::channel::TrySendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<hdrhistogram::CreationError> for Error {
    fn from(err: hdrhistogram::CreationError) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<hdrhistogram::RecordError> for Error {
    fn from(err: hdrhistogram::RecordError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<log::ParseLevelError> for Error {
    fn from(err: log::ParseLevelError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<rand::distr::uniform::Error> for Error {
    fn from(err: rand::distr::uniform::Error) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        panic!("{err}") // faulty code
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        // This only happens when a different thread panics while holding a
        // mutex. This should be fatal, so we panic here too.
        panic!("{err}")
    }
}

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    fn take_bytes(&mut self, len: usize) -> Result<&[u8]> {
        if self.input.len() < len {
            return errdata!(
                "insufficient bytes, expected {len} bytes for {:x?}",
                self.input
            );
        }
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        Ok(bytes)
    }

    fn decode_next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut decoded = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,
                    Some((_, 0xff)) => decoded.push(0x00),
                    _ => return errdata!("invalid escape sequence"),
                },
                Some((_, b)) => decoded.push(*b),
                None => return errdata!("unexpected end of input"),
            }
        };
        self.input = &self.input[taken..];
        Ok(decoded)
    }
}

impl<'de> serde::de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        panic!("must provide type, Keycode is not self-describing")
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(match self.take_bytes(1)?[0] {
            0x00 => false,
            0x01 => true,
            b => return errdata!("invalid boolean value {b}"),
        })
    }

    fn deserialize_i8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7; // flip sign bit
        visitor.visit_i64(i64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::from_be_bytes(self.take_bytes(8)?.try_into()?))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        match bytes[0] >> 7 {
            0 => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
            1 => bytes[0] ^= 1 << 7,                      // positive, flip sign bit
            _ => panic!("bits can only be 0 or 1"),
        }
        visitor.visit_f64(f64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_string(String::from_utf8(bytes)?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }
}

/// Sequences are simply deserialized until the byte slice is exhausted.
impl<'de> SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.input.is_empty() {
            return Ok(None);
        }
        seed.deserialize(self).map(Some)
    }
}

/// Enum variants are deserialized by their index.
impl<'de> EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let index = self.take_bytes(1)?[0] as u32;
        let value: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((value?, self))
    }
}

/// Enum variant contents are deserialized as sequences.
impl<'de> VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn struct_variant<V: Visitor<'de>>(self, _: &'static [&'static str], _: V) -> Result<V::Value> {
        unimplemented!()
    }
}
