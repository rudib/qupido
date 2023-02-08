use std::any::TypeId;
use std::{collections::HashMap, sync::Arc, any::Any};
use std::fmt::Debug;

use crate::{QupidoResult, QupidoError};

#[derive(Clone, Debug)]
pub struct Container<T> {
    pub data: HashMap<String, Arc<T>>
}

pub trait ContainerData: Any + Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;    
}

impl<T> ContainerData for T where T: Any + Debug {
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

impl<T> Container<T> {
    pub fn new() -> Self {
        Container {
            data: HashMap::default()
        }
    }

    pub fn insert(&mut self, key: &str, value: T) -> QupidoResult
    {
        if self.data.contains_key(key) {
            return Err(QupidoError::DuplicateData(key.to_string()));
        }

        self.upsert(key, value);

        Ok(())
    }

    pub fn upsert(&mut self, key: &str, value: T)
    {
        self.data.insert(key.to_string(), Arc::new(value));
    }

    pub fn get(&self, key: &str) -> QupidoResult<&T>
    {
        let v = self.data.get(key).ok_or(QupidoError::DataNotFound(key.to_string()))?;
        Ok(v.as_ref())
    }
}