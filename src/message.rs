// Automatically generated rust module for 'message.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use std::io::Write;
use std::borrow::Cow;
use quick_protobuf::{MessageRead, MessageWrite, BytesReader, Writer, Result};
use quick_protobuf::sizeofs::*;
use super::*;

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ChannelRowMessage<'a> {
    pub ids: Vec<i32>,
    pub serials: Vec<Cow<'a, str>>,
}

impl<'a> MessageRead<'a> for ChannelRowMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.ids = r.read_packed(bytes, |r, bytes| Ok(r.read_int32(bytes)?))?,
                Ok(18) => msg.serials.push(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for ChannelRowMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.ids.is_empty() { 0 } else { 1 + sizeof_len(self.ids.iter().map(|s| sizeof_varint(*(s) as u64)).sum::<usize>()) }
        + self.serials.iter().map(|s| 1 + sizeof_len((s).len())).sum::<usize>()
    }

    fn write_message<W: Write>(&self, w: &mut Writer<W>) -> Result<()> {
        w.write_packed_with_tag(10, &self.ids, |w, m| w.write_int32(*m), &|m| sizeof_varint(*(m) as u64))?;
        for s in &self.serials { w.write_with_tag(18, |w| w.write_string(&**s))?; }
        Ok(())
    }
}

