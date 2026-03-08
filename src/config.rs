use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    Create(ServerConfigArgs),
    Open(ServerConfigArgs),
}

#[derive(Debug, Clone, Parser)]
pub struct ServerConfigArgs {
    #[arg(long)]
    pub export_id: String,
    #[arg(long)]
    pub cache_dir: PathBuf,
    #[arg(long)]
    pub bucket: String,
    #[arg(long)]
    pub prefix: String,
    #[arg(long)]
    pub listen: SocketAddr,
    #[arg(long)]
    pub admin_sock: PathBuf,
    #[arg(long, value_enum, default_value_t = StorageBackendKind::S3)]
    pub storage_backend: StorageBackendKind,
    #[arg(long, default_value = "us-east-1")]
    pub region: String,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long)]
    pub r2_account_id: Option<String>,
    #[arg(long, default_value_t = CHUNK_SIZE)]
    pub chunk_size: u64,
    #[arg(long)]
    pub snapshot_id: Option<u64>,
    #[arg(long)]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StorageBackendKind {
    S3,
    R2,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackendKind,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub r2_account_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub export_id: String,
    pub cache_dir: PathBuf,
    pub storage: StorageConfig,
    pub listen: SocketAddr,
    pub admin_sock: PathBuf,
    pub chunk_size: u64,
    pub snapshot_id: Option<u64>,
    pub image_size: Option<u64>,
}

impl From<ServerConfigArgs> for ServerConfig {
    fn from(value: ServerConfigArgs) -> Self {
        Self {
            export_id: value.export_id,
            cache_dir: value.cache_dir,
            storage: StorageConfig {
                backend: value.storage_backend,
                bucket: value.bucket,
                prefix: value.prefix.trim_end_matches('/').to_string(),
                region: value.region,
                endpoint_url: value.endpoint_url,
                r2_account_id: value.r2_account_id,
            },
            listen: value.listen,
            admin_sock: value.admin_sock,
            chunk_size: value.chunk_size,
            snapshot_id: value.snapshot_id,
            image_size: value.size,
        }
    }
}
