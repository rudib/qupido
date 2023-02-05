use std::{collections::HashMap, any::{Any, TypeId}, sync::Arc, ops::Add};

use petgraph::{Graph, algo::{toposort}, visit::{NodeIndexable}};
use uuid::Uuid;
use std::fmt::Debug;

pub mod node;
pub mod pipeline;
pub mod source;
pub mod container;

#[derive(Clone, Debug)]
pub enum Tag {
    Tag(String)
}

pub fn tag(tag: impl Into<String>) -> Tag {
    Tag::Tag(tag.into())
}




#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Source {
    Id(String)
}

impl Source {
    pub fn get_id(&self) -> String {
        match self {
            Source::Id(id) => id.clone(),
        }
    }
}

pub fn id(s: impl Into<String>) -> Source {
    Source::Id(s.into())
}




pub struct Context {
    pub inputs: crate::container::Container
}

impl Context {
    
}


#[derive(Debug, Clone)]
pub enum QupidoError {
    DataNotFound(String),
    InvalidPipeline,
    DuplicateData(String),
    DataTypeMismatch {
        id: String,
        requested: TypeId,
        stored: TypeId
    },
    NodeNotFound,
}

pub type QupidoResult<T = ()> = Result<T, QupidoError>;

