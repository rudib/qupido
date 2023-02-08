use std::{collections::HashMap, any::{Any, TypeId}, sync::Arc, ops::Add};

use petgraph::{Graph, algo::{toposort}, visit::{NodeIndexable}};
use uuid::Uuid;
use std::fmt::Debug;

use crate::{source::NodeSources, Tag, Context, QupidoResult, container::Container, tag};


#[derive(Clone, Debug)]
pub struct Node<T> {
    pub id: Uuid,
    pub inputs: NodeSources,
    pub outputs: NodeSources,
    pub tags: Vec<Tag>,
    pub func: NodeFunc<T>,
    pub namespace: Option<String>
}

impl<T> Node<T> where T: Clone {
    pub fn new<F>(inputs: impl Into<NodeSources>, outputs: impl Into<NodeSources>, func: F) -> Self
        where F: Fn(&Context<T>) -> QupidoResult<Container<T>> + 'static
    {
        Node {
            id: Uuid::new_v4(),
            inputs: inputs.into(),
            outputs: outputs.into(),
            tags: vec![],
            func: NodeFunc { f: Arc::new(Box::new(func)) },
            namespace: None
        }
    }

    pub fn tag(self, simple_tag: impl Into<String>) -> Self {
        let mut s = self.clone();
        s.tags.push(tag(simple_tag));
        s
    }
}

#[derive(Clone)]
pub struct NodeFunc<T> {
    pub f: Arc<Box<dyn Fn(&Context<T>) -> QupidoResult<Container<T>>>>
}

impl<T> std::fmt::Debug for NodeFunc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunc").field("f", &"some func").finish()
    }
}