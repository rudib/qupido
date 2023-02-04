use std::{collections::HashMap, any::Any};

use petgraph::{Graph, algo::{toposort, tred::dag_to_toposorted_adjacency_list}, adj::NodeIndex, visit::IntoNeighbors};
use uuid::Uuid;




#[derive(Clone, Debug)]
pub struct Node {
    pub id: Uuid,
    pub inputs: Vec<Source>,
    pub outputs: Vec<Source>
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Source {
    Id(String)
}


pub struct Pipeline {
    nodes: Vec<Node>
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
                let node_source_id = outputs.get(i).expect("No source node found");
                let src = graph_nodes.get(node_source_id).expect("Source node not found");
                g.add_edge(src.clone(), dst.clone(), i.clone());
            }
        }

        //println!("g={:#?}", g);
        
        let sorted = toposort(&g, None).expect("Can't topologically sort");

        Pipeline {
            nodes: sorted.into_iter()
                        .map(|idx| nodes.get(idx.index()).unwrap().clone())
                        .collect()
        }

        
        
    }
}


pub struct Container {
    pub data: HashMap<String, Box<dyn Any>>
}




#[test]
fn test_nodes_topo() {
    let a = Node {
        id: Uuid::new_v4(),
        inputs: vec![],
        outputs: vec![Source::Id("foo".into())]
    };

    let b = Node {
        id: Uuid::new_v4(),
        inputs: vec![Source::Id("foo".into())],
        outputs: vec![Source::Id("zzz".into())]
    };

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
    

    let pipeline = Pipeline::from_nodes(&[a, b, c, d, e, f]);


    


}