use dotenv::dotenv;
use sqlx::PgPool;

pub async fn init() -> PgPool {
    dotenv().ok();
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE URL MUST BE SET.");

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .unwrap_or_else(|_| {
            panic!("Failed create connection pool.")
        })
}