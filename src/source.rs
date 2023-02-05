use std::collections::HashMap;

use crate::Source;


#[derive(Debug, Clone)]
pub enum NodeSources {
    List(Vec<Source>),
    Map(HashMap<Source, Source>)
}

impl NodeSources {
    pub fn inputs(&self) -> Vec<Source> {
        match self {
            NodeSources::List(l) => l.clone(),
            NodeSources::Map(m) => m.values().cloned().collect(),
        }
    }

    pub fn outputs(&self) -> Vec<Source> {
        match self {
            NodeSources::List(l) => l.clone(),
            NodeSources::Map(m) => m.values().cloned().collect(), // is this right?
        }
    }

    pub fn map<F>(&self, map: F) -> Self
        where F: Fn(&Source) -> Source
    {
        match self {
            NodeSources::List(l) => {
                let mut h = HashMap::new();
                for node_id in l {
                    let global_id = map(node_id);
                    h.insert(node_id.clone(), global_id);
                }

                NodeSources::Map(h) 
            },
            NodeSources::Map(m) => {
                let mut h = HashMap::new();
                for (node_id, global_id) in m {
                    let new_global_id = map(global_id);
                    h.insert(node_id.clone(), new_global_id);
                }
                NodeSources::Map(h)
            },
        }
    }
}

impl From<&[Source]> for NodeSources {
    fn from(value: &[Source]) -> Self {
        NodeSources::List(value.to_vec())
    }
}

impl<const N: usize> From<[Source; N]> for NodeSources {
    fn from(value: [Source; N]) -> Self {
        NodeSources::List(value.to_vec())
    }
}

impl From<&[(Source, Source)]> for NodeSources {
    fn from(value: &[(Source, Source)]) -> Self {
        let mut m = HashMap::new();
        for (k, v) in value {
            m.insert(k.clone(), v.clone());
        }
        NodeSources::Map(m)
    }
}

impl<const N: usize> From<[(Source, Source); N]> for NodeSources {
    fn from(value: [(Source, Source); N]) -> Self {
        NodeSources::Map(HashMap::from(value))
    }
}

impl From<()> for NodeSources {
    fn from(value: ()) -> Self {
        Self::List(vec![])
    }
}





