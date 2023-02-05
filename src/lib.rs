use std::{collections::HashMap, any::{Any, TypeId}, sync::Arc, ops::Add};

use petgraph::{Graph, algo::{toposort}, visit::{NodeIndexable}};
use uuid::Uuid;
use std::fmt::Debug;


#[derive(Clone, Debug)]
pub enum Tag {
    Tag(String)
}

pub fn tag(tag: impl Into<String>) -> Tag {
    Tag::Tag(tag.into())
}


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
            NodeSources::Map(m) => m.values().cloned().collect(),
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


#[derive(Clone, Debug)]
pub struct Node {
    pub id: Uuid,
    pub inputs: NodeSources,
    pub outputs: NodeSources,
    pub tags: Vec<Tag>,
    pub func: NodeFunc
}

impl Node {
    pub fn new<F>(inputs: impl Into<NodeSources>, outputs: impl Into<NodeSources>, func: F) -> Self
        where F: Fn(&Context) -> QupidoResult<Container> + 'static
    {
        Node {
            id: Uuid::new_v4(),
            inputs: inputs.into(),
            outputs: outputs.into(),
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
    pub f: Arc<Box<dyn Fn(&Context) -> QupidoResult<Container>>>
}

impl std::fmt::Debug for NodeFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunc").field("f", &"some func").finish()
    }
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


#[derive(Debug)]
pub struct Pipeline {
    nodes: Vec<Node>,
    graph: Graph<Uuid, Source>
}

impl Pipeline {
    pub fn from_nodes(nodes: &[Node]) -> QupidoResult<Pipeline> {
        let mut g = Graph::<Uuid, Source>::new();
        
        let graph_nodes: HashMap<Uuid, _> = nodes.iter()
            .map(|n| (n.id, g.add_node(n.id)))
            .collect();

        let mut outputs = HashMap::new();
        for n in nodes {
            for o in &n.outputs.outputs() {
                if let Some(_) = outputs.insert(o.clone(), n.id) {
                    return Err(QupidoError::DuplicateData(o.get_id()));
                }
            }
        }
        for n in nodes {
            let dst = graph_nodes.get(&n.id).ok_or(QupidoError::NodeNotFound)?;
            
            for i in n.inputs.inputs() {
                if let Some(node_source_id) = outputs.get(&i) {
                    let src = graph_nodes.get(node_source_id).ok_or(QupidoError::NodeNotFound)?;
                    g.add_edge(src.clone(), dst.clone(), i.clone());
                }
            }
        }

        let sorted = toposort(&g, None).map_err(|e| QupidoError::InvalidPipeline)?;

        Ok(Pipeline {
            nodes: sorted.into_iter()
                        .map(|idx| nodes.get(idx.index()).unwrap().clone())
                        .collect(),
            graph: g
        })
    }

    pub fn run(&self, container: &Container) -> QupidoResult<Container> {

        let mut container_run_state = container.clone();

        for n in &self.nodes {

            // remap
            let container_input = {
                let mut c = container_run_state.clone();
                match &n.inputs {
                    NodeSources::List(_) => (),
                    NodeSources::Map(m) => {
                        for (node_id, global_id) in m {
                            let v = c.data.get(&global_id.get_id()).ok_or(QupidoError::DataNotFound(global_id.get_id()))?;
                            c.data.insert(node_id.get_id(), v.clone());
                        }
                    },
                }
                c
            };
            let ctx = Context {
                inputs: container_input
            };
            let mut res = (n.func.f)(&ctx)?;
            
            match &n.outputs {
                NodeSources::List(l) => {
                    for o in l {
                        let val = res.data.remove(&o.get_id()).ok_or(QupidoError::DataNotFound(o.get_id()))?;
                        container_run_state.data.insert(o.get_id(), val);
                    }
                },
                NodeSources::Map(m) => {
                    for (node_id, global_id) in m {
                        let val = res.data.remove(&node_id.get_id()).ok_or(QupidoError::DataNotFound(node_id.get_id()))?;
                        container_run_state.data.insert(global_id.get_id(), val);
                    }
                },
            }

        }

        Ok(container_run_state)
    }

    pub fn inputs(&self) -> Vec<Source> {
        let mut r = vec![];
        
        for n in &self.nodes {
            let idx = self.graph.node_weights().position(|id| n.id == *id).ok_or(QupidoError::NodeNotFound).unwrap();
            let idx = self.graph.from_index(idx);
        
            let incoming_edges = self.graph.edges_directed(idx, petgraph::Direction::Incoming).collect::<Vec<_>>();
        
            for input in &n.inputs.inputs() {
                if !incoming_edges.iter().any(|e| e.weight() == input) {
                    r.push(input.clone());
                }
            }
        }

        r.sort();
        r
    }

    pub fn outputs(&self) -> Vec<Source> {
        let mut r = vec![];
        
        for n in &self.nodes {
            let idx = self.graph.node_weights().position(|id| n.id == *id).ok_or(QupidoError::NodeNotFound).unwrap();
            let idx = self.graph.from_index(idx);
        
            let outgoing_edges = self.graph.edges_directed(idx, petgraph::Direction::Outgoing).collect::<Vec<_>>();
        
            for output in &n.outputs.outputs() {
                if !outgoing_edges.iter().any(|e| e.weight() == output) {
                    r.push(output.clone());
                }
            }
        }

        r.sort();
        r
    }

    pub fn all_outputs(&self) -> Vec<Source> {
        let mut r = vec![];
        for n in &self.nodes {
            for output in &n.outputs.outputs() {
                if !r.contains(output) {
                    r.push(output.clone());
                }
            }
        }

        r.sort();
        r
    }

    pub fn add(&self, other: &Pipeline) -> QupidoResult<Pipeline> {
        let mut a = self.nodes.clone();
        a.extend_from_slice(&other.nodes);
        Self::from_nodes(a.as_slice())
    }
}


pub struct Context {
    pub inputs: Container
}

impl Context {
    
}

#[derive(Clone, Debug)]
pub struct Container {
    data: HashMap<String, Arc<Box<dyn ContainerData>>>
}

pub trait ContainerData: Any + Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;    
}

impl<T> ContainerData for T where T: Any + Debug {
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

impl Container {
    pub fn new() -> Self {
        Container {
            data: HashMap::default()
        }
    }

    pub fn insert<V>(&mut self, key: &str, value: V) -> QupidoResult
        where V: ContainerData + 'static 
    {
        if self.data.contains_key(key) {
            return Err(QupidoError::DuplicateData(key.to_string()));
        }

        self.upsert(key, value);

        Ok(())
    }

    pub fn upsert<V>(&mut self, key: &str, value: V)
        where V: ContainerData + 'static 
    {
        self.data.insert(key.to_string(), Arc::new(Box::new(value)));
    }

    pub fn get<V>(&self, key: &str) -> QupidoResult<&V>
        where V: ContainerData + 'static
    {
        let v = self.data.get(key).ok_or(QupidoError::DataNotFound(key.to_string()))?;
        let v = (***v).as_any();

        if let Some(v) = v.downcast_ref() {
            Ok(v)
        } else {
            Err(QupidoError::DataTypeMismatch { id: key.to_string(), requested: TypeId::of::<V>(), stored: v.type_id() })
        }
    }
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




#[test]
fn test_nodes_topo() -> QupidoResult {


    let a = Node::new([(id("param_a"), id("a")), (id("param_b"), id("b"))],
            [
                (id("out_plus"), id("a_plus_b")),
                (id("out_times"), id("a_times_b"))
            ],
        |ctx| {
            let a: &u32 = ctx.inputs.get("param_a")?;
            let b: &u32 = ctx.inputs.get("param_b")?;

            let mut r = Container::new();
            r.insert("out_plus", a + b)?;
            r.insert("out_times", a * b)?;
            Ok(r)
        }).tag("math").tag("plus").tag("multiply");

    let b = Node::new([id("a_plus_b")], [id("squared")], |ctx| {
        let v = ctx.inputs.get::<u32>("a_plus_b")?;
        let mut r = Container::new();
        r.insert("squared", v * v)?;
        Ok(r)
    }).tag("math");

    let c = Node::new([id("squared")], [id("squared_plus_1")], |ctx| {
        let v = ctx.inputs.get::<u32>("squared")?;
        let mut r = Container::new();
        r.insert("squared_plus_1", v + 1)?;
        Ok(r)
    }).tag("math");
    

    let pipeline = Pipeline::from_nodes(&[a, b, c])?;
    println!("pipeline={:#?}", pipeline);

    let container = {
        let mut c = Container::new();
        c.insert("a", 3 as u32)?;
        c.insert("b", 5 as u32)?;
        c
    };
    
    {
        assert_eq!(pipeline.inputs(), vec![id("a"), id("b")]);
        assert_eq!(pipeline.outputs(), vec![id("a_times_b"), id("squared_plus_1")]);
        assert_eq!(pipeline.all_outputs(), vec![id("a_plus_b"), id("a_times_b"), id("squared"), id("squared_plus_1")]);

        let result = pipeline.run(&container)?;
        println!("result: {:#?}", result);

        let a_plus_b = result.get::<u32>("a_plus_b")?;
        assert_eq!(*a_plus_b, 8);
    }


    let pb = Pipeline::from_nodes(&[
        Node::new([id("squared_plus_1")], (), |ctx| {
            println!("val: {:?}", ctx.inputs.get::<u32>("squared_plus_1")?);
            Ok(Container::new())
        })
    ])?;

    let full = pipeline.add(&pb)?;
    {
        let result = full.run(&container)?;
    }

        
    Ok(())
}

#[test]
fn test_namespaces() -> QupidoResult {

    pub fn num_calc<T>() -> QupidoResult<Pipeline>
        where T: Add<T, Output = T> + Debug + Clone + 'static
    {
        let n = Node::new([id("x"), id("y")], [id("x+y")],
        |ctx| {
            let x = ctx.inputs.get::<T>("x")?;
            let y = ctx.inputs.get::<T>("y")?;

            let mut r = Container::new();
            r.insert("x+y", x.clone() + y.clone())?;
            Ok(r)
        });
        Pipeline::from_nodes(&[n])
    }

    let x_y = num_calc::<i64>()?;
    let mut data = Container::new();
    data.insert("x", -3 as i64)?;
    data.insert("y", 12 as i64)?;

    let data_result = x_y.run(&data)?;
    assert_eq!(*data_result.get::<i64>("x+y")?, 9 as i64);


    


    Ok(())
}