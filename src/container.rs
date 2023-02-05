use std::any::TypeId;
use std::{collections::HashMap, sync::Arc, any::Any};
use std::fmt::Debug;

use crate::{QupidoResult, QupidoError};

#[derive(Clone, Debug)]
pub struct Container {
    pub data: HashMap<String, Arc<Box<dyn ContainerData>>>
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