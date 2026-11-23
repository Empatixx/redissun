//! redissun's own connection layer: it opens every connection and speaks Redis through redis-rs.
// Nothing uses the layer until the pooled connections move onto it; this goes then.
#![allow(dead_code)]

pub(crate) mod node;
pub(crate) mod stream;
