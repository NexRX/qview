use crate::*;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::{Duration, Instant};
use testcontainers::{
    ContainerRequest, GenericImage, ImageExt,
    core::{IntoContainerPort as _, WaitFor, logs::LogFrame},
    runners::AsyncRunner as _,
};
use tokio::sync::OnceCell;

pub type Container = testcontainers::ContainerAsync<GenericImage>;

const PG_USER: &str = "postgres";
const PG_PASS: &str = "postgres";

// --- Container Singleton ---
pub async fn postgres() -> &'static Container {
    static POSTGRES: OnceCell<Container> = OnceCell::const_new();
    const TRIES: u8 = 5;
    POSTGRES
        .get_or_init(|| async {
            for attempt in 1..=TRIES {
                match container().await {
                    Ok(container) => return container,
                    Err(e) => {
                        error!("Attempt {attempt}/{TRIES} failed: {e:?}");
                        if attempt == TRIES {
                            error!("Fatal: All attempts failed");
                            std::process::exit(1);
                        }
                    }
                }
            }
            unreachable!()
        })
        .await
}

// --- Pool Helpers ---
/// Create a new PostgreSQL connection pool to the test container.
pub(super) async fn pool(database: &str) -> PgPool {
    let container: &Container = postgres().await;
    let con_str = format!(
        "postgres://{PG_USER}:{PG_PASS}@{}:{}/{database}",
        container.get_host().await.expect("container host"),
        container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port")
    );
    PgPoolOptions::new()
        .max_connections(3)
        .connect(&con_str)
        .await
        .expect("db init connection failure")
}

// --- Container Setup ---
async fn container() -> Result<Container> {
    debug!("Starting Postgres DB Container");
    let container_startup = Instant::now();
    let container = image().start().await.expect("db startup failure");
    let container_startup = container_startup.elapsed();
    debug!("Container ready in {:#.2?}", container_startup);
    Ok(container)
}

fn image() -> ContainerRequest<GenericImage> {
    const PG_INIT_SQL: &[u8] = b" -- Initialize Postgres
        ALTER SYSTEM SET fsync = off;
        ALTER SYSTEM SET synchronous_commit = off;
        ALTER SYSTEM SET full_page_writes = off;
        ALTER SYSTEM SET shared_buffers = '128MB';
        ALTER SYSTEM SET max_wal_size = '128MB';
        ALTER SYSTEM SET work_mem = '16MB';
        ALTER SYSTEM SET maintenance_work_mem = '64MB';
        ALTER SYSTEM SET wal_level = 'replica';";

    const fn gb(gb: u64) -> u64 {
        gb * 1024 * 1024 * 1024
    }

    let mut image = GenericImage::new("kartoza/postgis", "14")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("listening on IPv6 address"))
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_copy_to("/docker-entrypoint-initdb.d/init.sql", PG_INIT_SQL.to_vec())
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASS)
        .with_env_var("POSTGRES_DB", "postgres");

    if config().container_logs {
        image = image.with_log_consumer(|line: &LogFrame| trace!("[Container Logs] {line:?}"));
    }

    if config().container_ramdisked {
        image = image // 4x speedup 🔥🔥🔥🔥
            .with_env_var("PGDATA", "/dev/shm/pgdata")
            .with_shm_size(gb(2)); // NOTE: Increase if test db runs out of space
    }

    image.with_startup_timeout(Duration::from_secs(60))
}
