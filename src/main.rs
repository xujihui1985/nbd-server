use clap::Parser;
use nbd_server::admin::serve_admin;
use nbd_server::config::ServerConfig;
use nbd_server::remote::build_storage_backend;
use nbd_server::{Cli, Commands};

#[tokio::main]
async fn main() -> nbd_server::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let (config, create_mode) = match cli.command {
        Commands::Create(args) => (ServerConfig::from(args), true),
        Commands::Open(args) => (ServerConfig::from(args), false),
    };

    let remote = build_storage_backend(&config.storage).await?;
    let export = if create_mode {
        nbd_server::export::Export::create(config.clone(), remote).await?
    } else {
        nbd_server::export::Export::open(config.clone(), remote).await?
    };

    let admin_socket = config.admin_sock.clone();
    let nbd_addr = config.listen;
    let admin_export = export.clone();
    let nbd_export = export.clone();

    let admin_task = tokio::spawn(async move { serve_admin(&admin_socket, admin_export).await });
    let nbd_task =
        tokio::spawn(async move { nbd_server::nbd::serve_nbd(nbd_addr, nbd_export).await });

    tokio::select! {
        result = admin_task => {
            export.shutdown()?;
            result.map_err(|error| nbd_server::Error::Io(std::io::Error::other(error.to_string())))??;
        }
        result = nbd_task => {
            export.shutdown()?;
            result.map_err(|error| nbd_server::Error::Io(std::io::Error::other(error.to_string())))??;
        }
        _ = tokio::signal::ctrl_c() => {
            export.shutdown()?;
        }
    }

    Ok(())
}
