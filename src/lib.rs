use std::{collections::HashMap, any::{Any, TypeId}, sync::Arc};

use petgraph::{Graph, algo::{toposort, tred::dag_to_toposorted_adjacency_list}, adj::NodeIndex, visit::{IntoNeighbors, Dfs, Topo, Walker, NodeIndexable}};
use uuid::Uuid;




#[derive(Clone, Debug)]
pub struct Node {
    pub id: Uuid,
    pub inputs: Vec<Source>,
    pub outputs: Vec<Source>,
    pub func: NodeFunc
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

    pub fn free_inputs(&self) -> Vec<Source> {
        todo!()
    }

    pub fn outputs(&self) -> Vec<Source> {
        let mut r = vec![];
        //let topo = Topo::new(&self.graph).iter(&self.graph);
        
        //for idx in topo {
        for n in &self.nodes {
            let idx = self.graph.node_weights().position(|id| n.id == *id).expect("ha?");            
            let idx = self.graph.from_index(idx);
            println!("{} => {:?}", n.id, idx);

            let outgoing_edges = self.graph.edges_directed(idx, petgraph::Direction::Outgoing).collect::<Vec<_>>();
            println!("{:#?}", outgoing_edges);

            for output in &n.outputs {
                if !outgoing_edges.iter().any(|e| e.weight() == output) {
                    r.push(output.clone());
                }
            }
            //let node_id = self.graph[idx];
            //let node = self.nodes.in
        }
        r
    }

    pub fn all_outputs(&self) -> Vec<Source> {
        todo!()
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
    let a = Node {
        id: Uuid::new_v4(),
        inputs: vec![id("a"), id("b")],
        outputs: vec![id("c")],
        func: NodeFunc { f: 
            Arc::new(Box::new(
                |c| {
                    let a: &u32 = c.inputs.get("a");
                    let b: &u32 = c.inputs.get("b");

                    let res = a + b;

                    let mut r = Container::new();
                    r.insert("c", res);
                    r
                }
            ))
        }
    };

    /*
    let b = Node {
        id: Uuid::new_v4(),
        inputs: vec![id("c")],
        outputs: vec![],
        func: NodeFunc { f: Arc::new(Box::new(|ctx| {
            let c = ctx.inputs.get::<u32>("c");

            println!("c={}", c);
            Container::new()
        })) }
    };
    */

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
        c.insert("a", 2 as u32);
        c.insert("b", 2 as u32);
        c
    };

    let pipeline = Pipeline::from_nodes(&[a]);

    assert_eq!(pipeline.outputs(), vec![id("c")]);








    let result = pipeline.run(container);

    let c = result.get::<u32>("c");
    assert_eq!(*c, 4);

    
        
    


}