use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard}
};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use anyhow::Context;

#[derive(Debug, Error)]
enum RepositoryError {
    #[error("NotFound, id is {0}")]
    NotFound(i32)
}

pub trait TodoRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    fn create(&self, payload: CreateTodo) -> Todo;
    fn find(&self, id: i32) -> Option<Todo>;
    fn all(&self) -> Vec<Todo>;
    fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo>;
    fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Todo {
    id: i32,
    text: String,
    completed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTodo {
    text: String,
}

#[cfg(test)]
impl CreateTodo {
    pub fn new(text: String) -> Self {
        Self { text }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateTodo {
    text: Option<String>,
    completed: Option<bool>,
}

impl  Todo {
    pub fn new(id: i32, text: String) -> Self {
        Self {
            id,
            text,
            completed: false,
        }
    }
}

type TodoDatas = HashMap<i32, Todo>;

#[derive(Default, Debug, Clone,)]
pub struct TodoRepositoryForMemory {
    store: Arc<RwLock<TodoDatas>>,
}

impl TodoRepositoryForMemory {
    pub fn new() -> Self {
        TodoRepositoryForMemory { 
            store: Arc::default(),  
        }
    }

    fn write_store_ref(&self) -> RwLockWriteGuard<TodoDatas> {
        self.store.write().unwrap()
    }

    fn read_store_ref(&self) -> RwLockReadGuard<TodoDatas> {
        self.store.read().unwrap()
    }
}

impl TodoRepository for TodoRepositoryForMemory {
    fn create(&self, payload: CreateTodo) -> Todo {
        let mut store = self.write_store_ref();
        let id = (store.len() + 1) as i32;
        let todo = Todo::new(id, payload.text);
        store.insert(id, todo.clone());
        todo
    }

    fn find(&self, id: i32) -> Option<Todo> {
        let store = self.read_store_ref();
        store.get(&id).cloned()
    }

    fn all(&self) -> Vec<Todo> {
        let store = self.read_store_ref();
        Vec::from_iter(store.values().cloned())
    }

    fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo> {
        let mut store = self.write_store_ref();
        let todo = store.get(&id).context(RepositoryError::NotFound(id))?;
        let text = payload.text.unwrap_or_else(|| todo.text.clone());
        let completed = payload.completed.unwrap_or(todo.completed);
        let todo = Todo {
            id,
            text,
            completed,
        };
        store.insert(id, todo.clone());
        
        Ok(todo)
    }

    fn delete(&self, id: i32) -> anyhow::Result<()> {
        let mut store = self.write_store_ref();
        store.remove(&id).ok_or(RepositoryError::NotFound(id))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn todo_crud_scenario() {
        let text = "Test".to_string();
        let id = 1;
        let expected = Todo::new(id, text.clone());

        // CREATE
        let repository = TodoRepositoryForMemory::new();
        let todo = repository.create(CreateTodo { text });
        assert_eq!(expected, todo);

        // FIND
        let todo = repository.find(todo.id).unwrap();
        assert_eq!(expected, todo);

        // ALL
        let todo = repository.all();
        assert_eq!(vec![expected], todo);

        // UPDATE
        let text = "UPDATE TEST".to_string();
        let todo = repository.update(
            id,
            UpdateTodo { 
                text: Some(text.clone()),
                completed: Some(true) 
            },
        )
        .expect("Failed to update todo.");

        assert_eq!(
            Todo {
                id,
                text,
                completed: true,
            },
            todo
        );
        
        // DELETE
        let res = repository.delete(id);
        assert!(res.is_ok());
    }
}