use axum::async_trait;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use validator::Validate;
use sqlx::{FromRow, PgPool};

#[derive(Debug, Error)]
enum RepositoryError {
    #[error("Unexpected Error: [{0}]")]
    Unexpected(String),
    #[error("NotFound, id is {0}")]
    NotFound(i32),
}

#[derive(Debug, Clone)]
pub struct TodoRepositoryForDb {
    pool: PgPool,
}

impl TodoRepositoryForDb {
    pub fn new(pool: PgPool) -> Self {
        TodoRepositoryForDb { pool }
    }
}

#[async_trait]
pub trait TodoRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo>;
    async fn find(&self, id: i32) -> anyhow::Result<Todo>;
    async fn all(&self) -> anyhow::Result<Vec<Todo>>;
    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, FromRow)]
pub struct Todo {
    pub id: i32,
    pub text: String,
    pub completed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Validate)]
pub struct CreateTodo {
    #[validate(length(min = 1, message = "Can not be empty."))]
    #[validate(length(max = 100, message = "Over text length"))]
    text: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Validate)]
pub struct UpdateTodo {
    #[validate(length(min = 1, message = "Can not be empty."))]
    #[validate(length(max = 100, message = "Over text length"))]
    text: Option<String>,
    completed: Option<bool>,
}

#[async_trait]
impl TodoRepository for TodoRepositoryForDb {
    async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo> {
        let mut transaction = self.pool
            .begin()
            .await
            .unwrap();

        let todo = sqlx::query_file_as!(
                Todo,
                "sql/insertTodo.sql",
                payload.text.clone()
            )
            .fetch_one(&mut transaction)
            .await
            .unwrap_or_else(|_| {
                panic!("Failed to create todo.")
            });

        transaction
            .commit()
            .await
            .unwrap_or_else(|_| {
                panic!("Commit failed.")
            });

        Ok(todo)
    }

    async fn find(&self, id: i32) -> anyhow::Result<Todo> {
        let todo = sqlx::query_file_as!(
                Todo,
                "sql/findTodo.sql",
                id
            )
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
                _ => RepositoryError::Unexpected(e.to_string()),
            })?;

        Ok(todo)
    }

    async fn all(&self) -> anyhow::Result<Vec<Todo>> {
        let todo = sqlx::query_file_as!(
                Todo,
                "sql/allTodo.sql"
            )
            .fetch_all(&self.pool)
            .await?;
    
        Ok(todo)
    }

    async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo> {
        let mut transaction = self.pool
            .begin()
            .await
            .unwrap();

        let old_todo = self.find(id).await?;

        let todo = sqlx::query_file_as!(
                Todo,
                "sql/updateTodo.sql",
                payload.text.unwrap_or(old_todo.text),
                payload.completed.unwrap_or(old_todo.completed),
                id
            )
            .fetch_one(&mut transaction)
            .await
            .unwrap_or_else(|_| {
                panic!("Failed to update todo.")
            });

        transaction
            .commit()
            .await
            .unwrap_or_else(|_| {
                panic!("Commit failed.")
            });

        Ok(todo)
    }

    async fn delete(&self, id: i32) -> anyhow::Result<()> {
        let mut transaction = self.pool
            .begin()
            .await
            .unwrap();

        sqlx::query_file_as!(
                Todo,
                "sql/deleteTodo.sql",
                id
            )
            .execute(&mut transaction)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
                _ => RepositoryError::Unexpected(e.to_string()),
            })?;

        transaction
            .commit()
            .await
            .unwrap_or_else(|_| {
                panic!("Commit failed.")
            });

        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "database-test")]
mod test {
    use super::*;
    use dotenv::dotenv;
    use sqlx::PgPool;
    use std::env;

    async fn initialization_test_pool() -> PgPool {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL")
            .expect("DATABASE URL MUST BE SET.");

        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .unwrap_or_else(|_| {
                panic!("Failed create connection pool.")
            })
    }

    #[tokio::test]
    async fn crud_scenario() {
        let pool = initialization_test_pool().await;

        let repositry = TodoRepositoryForDb::new(pool.clone());
        let todo_text = "[crud_scenario] text";

        // create
        let created = repositry
            .create(CreateTodo::new(todo_text.to_string()))
            .await
            .expect("[create] returned Err");

        assert_eq!(created.text, todo_text);
        assert!(!created.completed);

        // find
        let todo = repositry
            .find(created.id)
            .await
            .expect("[find] returned Err");
        assert_eq!(created, todo);

        // all
        let todos = repositry.all().await.expect("[all] returned Err");
        let todo = todos.first().unwrap();
        assert_eq!(created, *todo);

        // update
        let updated_text = "[crud_scenario] update text";
        let todo = repositry
            .update(
                todo.id,
                UpdateTodo { 
                    text: Some(updated_text.to_string()),
                    completed: Some(true) 
                }
            )
            .await
            .expect("[update] returned Err");
        
        assert_eq!(created.id, todo.id);
        assert_eq!(todo.text, updated_text);

        // delete
        repositry
            .delete(todo.id)
            .await
            .expect("[delete] returned Err");

        let res = repositry.find(created.id).await;
        assert!(res.is_err());

        let todo_rows = sqlx::query_file_as!(
            Todo,
            "sql/findTodo.sql",
            todo.id
        )
        .fetch_all(&pool)
        .await
        .expect("[delete] todo_labes featch error");
        
        assert!(todo_rows.is_empty());
    }
}

#[cfg(test)]
pub mod test_utils {
    use anyhow::Context;
    use axum::async_trait;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    use super::*;

    impl Todo {
        pub fn new(id: i32, text: String) -> Self {
            Self {
                id,
                text,
                completed: false,
            }
        }
    }

    impl CreateTodo {
        pub fn new(text: String) -> Self {
            Self { text }
        }
    }

    type TodoDatas = HashMap<i32, Todo>;

    #[derive(Debug, Clone)]
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

    #[async_trait]
    impl TodoRepository for TodoRepositoryForMemory {
        async fn create(&self, payload: CreateTodo) -> anyhow::Result<Todo> {
            let mut store = self.write_store_ref();
            let id = (store.len() + 1) as i32;
            let todo = Todo::new(id, payload.text);
            store.insert(id, todo.clone());
            Ok(todo)
        }

        async fn find(&self, id: i32) -> anyhow::Result<Todo> {
            let store = self.read_store_ref();
            let todo = store
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::NotFound(id))?;
            Ok(todo)
        }

        async fn all(&self) -> anyhow::Result<Vec<Todo>> {
            let store = self.read_store_ref();
            Ok(Vec::from_iter(store.values().cloned()))
        }

        async fn update(&self, id: i32, payload: UpdateTodo) -> anyhow::Result<Todo> {
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

        async fn delete(&self, id: i32) -> anyhow::Result<()> {
            let mut store = self.write_store_ref();
            store.remove(&id).ok_or(RepositoryError::NotFound(id))?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[tokio::test]
        async fn todo_crud_scenario() {
            let text = "todo text".to_string();
            let id = 1;
            let expected = Todo::new(id, text.clone());

            // create
            let repository = TodoRepositoryForMemory::new();
            let todo = repository
                .create(CreateTodo { text })
                .await
                .expect("failed create todo");
            assert_eq!(expected, todo);

            // find
            let todo = repository.find(todo.id).await.unwrap();
            assert_eq!(expected, todo);

            // all
            let todo = repository.all().await.expect("failed get all todo");
            assert_eq!(vec![expected], todo);

            // update
            let text = "update todo text".to_string();
            let todo = repository
                .update(
                    1,
                    UpdateTodo {
                        text: Some(text.clone()),
                        completed: Some(true),
                    },
                )
                .await
                .expect("failed update todo.");
            assert_eq!(
                Todo {
                    id,
                    text,
                    completed: true,
                },
                todo
            );

            // delete
            let res = repository.delete(id).await;
            assert!(res.is_ok())
        }
    }
}