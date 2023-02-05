use std::{collections::HashMap, any::{Any, TypeId}, sync::Arc};

use petgraph::{Graph, algo::{toposort, tred::dag_to_toposorted_adjacency_list}, adj::NodeIndex, visit::{IntoNeighbors, Dfs, Topo, Walker, NodeIndexable}};
use uuid::Uuid;


#[derive(Clone, Debug)]
pub enum Tag {
    Tag(String)
}

pub fn tag(tag: impl Into<String>) -> Tag {
    Tag::Tag(tag.into())
}


#[derive(Clone, Debug)]
pub struct Node {
    pub id: Uuid,
    pub inputs: Vec<Source>,
    pub outputs: Vec<Source>,
    pub tags: Vec<Tag>,
    pub func: NodeFunc
}

impl Node {
    pub fn new<F>(inputs: &[Source], outputs: &[Source], func: F) -> Self
        where F: Fn(&Context) -> Container + 'static
    {
        Node {
            id: Uuid::new_v4(),
            inputs: inputs.to_vec(),
            outputs: outputs.to_vec(),
            tags: vec![],
            func: NodeFunc { f: Arc::new(Box::new(func)) }
        }
    }

    pub fn tag(self, simple_tag: impl Into<String>) -> Self {
        let mut s = self.clone();
        s.tags.push(tag(simple_tag));
        s
    }
}

#[derive(Clone)]
pub struct NodeFunc {
    pub f: Arc<Box<dyn Fn(&Context) -> Container>>
}

impl std::fmt::Debug for NodeFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunc").field("f", &"some func").finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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


#[derive(Debug)]
pub struct Pipeline {
    nodes: Vec<Node>,
    graph: Graph<Uuid, Source>
}

impl Pipeline {
    pub fn from_nodes(nodes: &[Node]) -> Pipeline {
        let mut g = Graph::<Uuid, Source>::new();
        
        let graph_nodes: HashMap<Uuid, _> = nodes.iter()
            .map(|n| (n.id, g.add_node(n.id)))
            .collect();

        let mut outputs = HashMap::new();
        for n in nodes {
            for o in &n.outputs {
                if let Some(_) = outputs.insert(o.clone(), n.id) {
                    panic!("Duplicate output");
                }
            }
        }
        for n in nodes {
            let dst = graph_nodes.get(&n.id).expect("Dest node not found");
            for i in &n.inputs {
                if let Some(node_source_id) = outputs.get(i) {
                    let src = graph_nodes.get(node_source_id).expect("Source node not found");
                    g.add_edge(src.clone(), dst.clone(), i.clone());
                }
            }
        }

        let sorted = toposort(&g, None).expect("Can't topologically sort");

        Pipeline {
            nodes: sorted.into_iter()
                        .map(|idx| nodes.get(idx.index()).unwrap().clone())
                        .collect(),
            graph: g
        }

        
        
    }

    pub fn run(&self, mut container: Container) -> Container {
        for n in &self.nodes {

            // todo: copy just the needed inputs?
            let ctx = Context {
                inputs: container
            };
            let mut res = (n.func.f)(&ctx);
            
            container = ctx.inputs;
            for o in &n.outputs {
                let val = res.data.remove(&o.get_id()).expect("Missing output?");
                container.data.insert(o.get_id(), val);
            }
        }

        container
    }

    pub fn inputs(&self) -> Vec<Source> {
        let mut r = vec![];
        
        for n in &self.nodes {
            let idx = self.graph.node_weights().position(|id| n.id == *id).expect("ha?");            
            let idx = self.graph.from_index(idx);
        
            let incoming_edges = self.graph.edges_directed(idx, petgraph::Direction::Incoming).collect::<Vec<_>>();
        
            for input in &n.inputs {
                if !incoming_edges.iter().any(|e| e.weight() == input) {
                    r.push(input.clone());
                }
            }
        }
        r
    }

    pub fn outputs(&self) -> Vec<Source> {
        let mut r = vec![];
        
        for n in &self.nodes {
            let idx = self.graph.node_weights().position(|id| n.id == *id).expect("ha?");            
            let idx = self.graph.from_index(idx);
        
            let outgoing_edges = self.graph.edges_directed(idx, petgraph::Direction::Outgoing).collect::<Vec<_>>();
        
            for output in &n.outputs {
                if !outgoing_edges.iter().any(|e| e.weight() == output) {
                    r.push(output.clone());
                }
            }
        }
        r
    }

    pub fn all_outputs(&self) -> Vec<Source> {
        let mut r = vec![];
        for n in &self.nodes {
            for output in &n.outputs {
                if !r.contains(output) {
                    r.push(output.clone());
                }
            }
        }
        r
    }
}


pub struct Context {
    pub inputs: Container
}

impl Context {
    
}

pub struct Container {
    data: HashMap<String, Box<dyn Any>>
}

impl Container {
    pub fn new() -> Self {
        Container {
            data: HashMap::default()
        }
    }

    pub fn insert<V>(&mut self, key: &str, value: V)
        where V: 'static 
    {
        if self.data.contains_key(key) {
            panic!("already exists");
        }

        self.upsert(key, value)
    }

    pub fn upsert<V>(&mut self, key: &str, value: V)
        where V: 'static 
    {
        self.data.insert(key.to_string(), Box::new(value));
    }

    pub fn get<V>(&self, key: &str) -> &V
        where V: 'static
    {
        let v = self.data.get(key).expect("id not found");
        
        if let Some(v) = v.downcast_ref() {
            v
        } else {
            let msg = format!("Requested type {:?}, stored type is {:?}", TypeId::of::<V>(), v.type_id());
            panic!("{}", msg);
        }
    }
}

pub enum QupidoError {

}

pub type QupidoResult<T = ()> = Result<T, QupidoError>;


#[test]
fn test_nodes_topo() {
    let a = Node::new(&[id("a"), id("b")], &[id("a_plus_b"), id("a_times_b")],
        |ctx| {
            let a: &u32 = ctx.inputs.get("a");
            let b: &u32 = ctx.inputs.get("b");

            let mut r = Container::new();
            r.insert("a_plus_b", a + b);
            r.insert("a_times_b", a * b);
            r
        }).tag("math").tag("plus").tag("multiply");

    let b = Node::new(&[id("a_plus_b")], &[id("squared")], |ctx| {
        let v = ctx.inputs.get::<u32>("a_plus_b");
        let mut r = Container::new();
        r.insert("squared", v * v);
        r
    }).tag("math");

    let c = Node::new(&[id("squared")], &[id("squared_plus_1")], |ctx| {
        let v = ctx.inputs.get::<u32>("squared");
        let mut r = Container::new();
        r.insert("squared_plus_1", v + 1);
        r
    }).tag("math");
    

    /*
    let c = Node {
        id: Uuid::new_v4(),
        inputs: vec![Source::Id("foo".into())],
        outputs: vec![Source::Id("bar".into())]
    };

    let d = Node {
        id: Uuid::new_v4(),
        inputs: vec![Source::Id("bar".into()), Source::Id("foo".into())],
        outputs: vec![Source::Id("xyz".into())]
    };

    let e = Node {
        id: Uuid::new_v4(),
        inputs: vec![Source::Id("zzz".into()), Source::Id("bar".into())],
        outputs: vec![Source::Id("123".into())]
    };

    let f = Node {
        id: Uuid::new_v4(),
        inputs: vec![],
        outputs: vec![Source::Id("456".into())]
    };
    */
    

    //let pipeline = Pipeline::from_nodes(&[a, b, c, d, e, f]);

    let container = {
        let mut c = Container::new();
        c.insert("a", 3 as u32);
        c.insert("b", 5 as u32);
        c
    };

    let pipeline = Pipeline::from_nodes(&[a, b, c]);

    println!("pipeline={:#?}", pipeline);

    assert_eq!(pipeline.inputs(), vec![id("a"), id("b")]);
    assert_eq!(pipeline.outputs(), vec![id("a_times_b"), id("squared_plus_1")]);
    assert_eq!(pipeline.all_outputs(), vec![id("a_plus_b"), id("a_times_b"), id("squared"), id("squared_plus_1")]);



    let result = pipeline.run(container);

    let a_plus_b = result.get::<u32>("a_plus_b");
    assert_eq!(*a_plus_b, 8);

    
        
    


}