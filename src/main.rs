use clap::Parser;
use nbd_server::Cli;
use nbd_server::admin::serve_manager_admin;
use nbd_server::config::ServeConfig;
use nbd_server::manager::ExportManager;
use nbd_server::remote::build_storage_backend;

#[tokio::main]
async fn main() -> nbd_server::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let serve = ServeConfig::from(cli.serve);
    let remote = build_storage_backend(&serve.storage).await?;
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

    Ok(())
}
