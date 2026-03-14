use clap::Parser;
use nbd_server::admin::{serve_admin, serve_manager_admin};
use nbd_server::config::{ServeConfig, ServerConfig};
use nbd_server::manager::ExportManager;
use nbd_server::remote::build_storage_backend;
use nbd_server::{Cli, Commands};

enum StartupMode {
    Create,
    Open,
    Clone,
    Serve,
}

#[tokio::main]
async fn main() -> nbd_server::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let (server_config, serve_config, mode) = match cli.command {
        Commands::Create(args) => (Some(ServerConfig::from(args)), None, StartupMode::Create),
        Commands::Open(args) => (Some(ServerConfig::from(args)), None, StartupMode::Open),
        Commands::Clone(args) => (Some(ServerConfig::from(args)), None, StartupMode::Clone),
        Commands::Serve(args) => (None, Some(ServeConfig::from(args)), StartupMode::Serve),
    };

    let storage_config = server_config
        .as_ref()
        .map(|config| &config.storage)
        .or_else(|| serve_config.as_ref().map(|config| &config.storage))
        .unwrap();
    let remote = build_storage_backend(storage_config).await?;

    if matches!(mode, StartupMode::Serve) {
        let serve = serve_config.unwrap();
        let manager = ExportManager::new(serve.clone(), remote).await?;
        let admin_socket = serve.admin_sock.clone();
        let nbd_addr = serve.listen;
        let admin_manager = manager.clone();
        let nbd_manager = manager.clone();
        let shutdown_manager = manager.clone();

        let admin_task =
            tokio::spawn(async move { serve_manager_admin(&admin_socket, admin_manager).await });
        let nbd_task =
            tokio::spawn(
                async move { nbd_server::nbd::serve_nbd_manager(nbd_addr, nbd_manager).await },
            );

        tokio::select! {
            result = admin_task => {
                shutdown_manager.shutdown_all().await?;
                result.map_err(|error| nbd_server::Error::Io(std::io::Error::other(error.to_string())))??;
            }
            result = nbd_task => {
                shutdown_manager.shutdown_all().await?;
                result.map_err(|error| nbd_server::Error::Io(std::io::Error::other(error.to_string())))??;
            }
            _ = tokio::signal::ctrl_c() => {
                shutdown_manager.shutdown_all().await?;
            }
        }
        return Ok(());
    }

    let config = server_config.unwrap();
    let export = match mode {
        StartupMode::Create => nbd_server::export::Export::create(config.clone(), remote).await?,
        StartupMode::Open => nbd_server::export::Export::open(config.clone(), remote).await?,
        StartupMode::Clone => {
            nbd_server::export::Export::clone_from_snapshot(config.clone(), remote).await?
        }
        StartupMode::Serve => unreachable!(),
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
