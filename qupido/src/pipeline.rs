use std::{collections::HashMap, ops::Add};
use std::fmt::Debug;

use petgraph::visit::NodeIndexable;
use petgraph::{Graph, algo::toposort};
use uuid::Uuid;

use crate::{node::Node, Source, QupidoResult, QupidoError, container::Container, source::NodeSources, Context, id};

#[derive(Debug)]
pub struct Pipeline<T> {
    nodes: Vec<Node<T>>,
    graph: Graph<Uuid, Source>
}

impl<T> Pipeline<T> where T: Clone {
    pub fn from_nodes(nodes: &[Node<T>]) -> QupidoResult<Pipeline<T>> {
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

    pub fn run(&self, container: &Container<T>) -> QupidoResult<Container<T>> {

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

    pub fn add(&self, other: &Pipeline<T>) -> QupidoResult<Pipeline<T>> {
        let mut a = self.nodes.clone();
        a.extend_from_slice(&other.nodes);
        Self::from_nodes(a.as_slice())
    }

    pub fn with_namespace(&self, namespace: &str) -> QupidoResult<Pipeline<T>> {
        let new_nodes: Vec<_> = self.nodes.iter().map(|n| {
            let mapping = |node_id: &Source| {
                let global_id = id(format!("{}.{}", namespace, node_id.get_id()));
                global_id
            };
            let new_inputs = n.inputs.map(mapping);
            let new_outputs = n.outputs.map(mapping);
            
            Node {
                id: Uuid::new_v4(),
                inputs: new_inputs,
                outputs: new_outputs,
                tags: n.tags.clone(),
                func: n.func.clone(),
                namespace: Some(n.namespace.as_ref().map(|ns| format!("{}.{}", namespace, ns)).unwrap_or(namespace.to_string()))
            }

        }).collect();

        Self::from_nodes(new_nodes.as_slice())
    }
}











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
        let v = ctx.inputs.get("a_plus_b")?;
        let mut r = Container::new();
        r.insert("squared", v * v)?;
        Ok(r)
    }).tag("math");

    let c = Node::new([id("squared")], [id("squared_plus_1")], |ctx| {
        let v = ctx.inputs.get("squared")?;
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

        let a_plus_b = result.get("a_plus_b")?;
        assert_eq!(*a_plus_b, 8);
    }


    let pb = Pipeline::from_nodes(&[
        Node::new([id("squared_plus_1")], (), |ctx| {
            println!("val: {:?}", ctx.inputs.get("squared_plus_1")?);
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

    pub fn num_calc<T>() -> QupidoResult<Pipeline<T>>
        where T: Add<T, Output = T> + Debug + Clone + 'static
    {
        let n = Node::new([id("x"), id("y")], [id("x+y")],
        |ctx| {
            let x: &T = ctx.inputs.get("x")?;
            let y: &T = ctx.inputs.get("y")?;

            let mut r = Container::new();
            r.insert("x+y", x.clone() + y.clone())?;
            Ok(r)
        });
        Pipeline::from_nodes(&[n])
    }

    pub fn num_calc_map<T>() -> QupidoResult<Pipeline<T>>
        where T: Add<T, Output = T> + Debug + Clone + 'static
    {
        let n = Node::new([(id("my_x"), id("x")), (id("my_y"), id("y"))], [(id("my_x+y"), id("x+y"))],
        |ctx| {
            let x: &T = ctx.inputs.get("my_x")?;
            let y: &T = ctx.inputs.get("my_y")?;

            let mut r = Container::new();
            r.insert("my_x+y", x.clone() + y.clone())?;
            Ok(r)
        });
        Pipeline::from_nodes(&[n])
    }

    let x_y = num_calc::<i64>()?;
    let mut data = Container::new();
    data.insert("x", -3 as i64)?;
    data.insert("y", 12 as i64)?;

    let data_result = x_y.run(&data)?;
    assert_eq!(*data_result.get("x+y")?, 9 as i64);

    let x_y_namespaced = num_calc::<i64>()?.with_namespace("calc")?;
    println!("namespaced={:#?}", x_y_namespaced);
    
    assert_eq!(x_y_namespaced.inputs(), vec![id("calc.x"), id("calc.y")]);
    assert_eq!(x_y_namespaced.outputs(), vec![id("calc.x+y")]);

    let mut data2 = Container::new();
    data2.insert("calc.x", 5 as i64)?;
    data2.insert("calc.y", 10 as i64)?;
    let data2_result = x_y_namespaced.run(&data2)?;
    assert_eq!(*data2_result.get("calc.x+y")?, 15);

    {
        let x_y = num_calc_map::<i64>()?;
        let mut data = Container::new();
        data.insert("x", -5 as i64)?;
        data.insert("y", 6 as i64)?;
    
        let data_result = x_y.run(&data)?;
        assert_eq!(*data_result.get("x+y")?, 1 as i64);

        let namespaced = x_y.with_namespace("foo")?;
        println!("namespaced: {:#?}", namespaced);
        let mut data = Container::new();
        data.insert("foo.x", -5 as i64)?;
        data.insert("foo.y", 6 as i64)?;
    
        let data_result = namespaced.run(&data)?;
        assert_eq!(*data_result.get("foo.x+y")?, 1 as i64);
    }

    Ok(())
}