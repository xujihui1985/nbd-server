use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

use crate::core::engine::export::Export;
use crate::core::error::{Error, Result};
use crate::core::nbd::codec::{
    ExportInfo, encode_info_reply, encode_option_reply, encode_reply, parse_export_name,
    parse_info_request, requested_infos,
};
use crate::core::nbd::protocol::{
    MAX_INFLIGHT_REQUESTS, NBD_CMD_DISC, NBD_CMD_FLAG_FUA, NBD_CMD_FLUSH, NBD_CMD_READ,
    NBD_CMD_WRITE, NBD_FLAG_FIXED_NEWSTYLE, NBD_FLAG_NO_ZEROES, NBD_IHAVEOPT, NBD_OPT_ABORT,
    NBD_OPT_EXPORT_NAME, NBD_OPT_GO, NBD_OPT_INFO, NBD_REP_ACK, NBD_REP_ERR_UNKNOWN,
    NBD_REP_ERR_UNSUP, NBD_REP_INFO, NBD_REQUEST_MAGIC, export_flags,
};
use crate::server::manager::ExportManager;

pub async fn serve_nbd_manager(
    addr: std::net::SocketAddr,
    manager: Arc<ExportManager>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let manager = manager.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_client_manager(stream, manager).await {
                tracing::warn!("nbd session ended with error: {error}");
            }
        });
    }
}

async fn handle_client_manager(mut stream: TcpStream, manager: Arc<ExportManager>) -> Result<()> {
    let client_flags = send_handshake(&mut stream).await?;
    let Some(export) = negotiate_options_manager(&mut stream, manager, client_flags).await? else {
        return Ok(());
    };
    transmission_phase(stream, export).await
}

async fn send_handshake(stream: &mut TcpStream) -> Result<u32> {
    stream
        .write_all(&crate::core::nbd::protocol::NBD_MAGIC.to_be_bytes())
        .await?;
    stream.write_all(&NBD_IHAVEOPT.to_be_bytes()).await?;
    stream
        .write_all(&(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES).to_be_bytes())
        .await?;

    let mut client_flags = [0_u8; 4];
    stream.read_exact(&mut client_flags).await?;
    Ok(u32::from_be_bytes(client_flags))
}

async fn negotiate_options_manager(
    stream: &mut TcpStream,
    manager: Arc<ExportManager>,
    client_flags: u32,
) -> Result<Option<Arc<Export>>> {
    loop {
        let mut header = [0_u8; 16];
        stream.read_exact(&mut header).await?;

        let magic = u64::from_be_bytes(header[0..8].try_into().unwrap());
        if magic != NBD_IHAVEOPT {
            return Err(Error::InvalidRequest(format!(
                "unexpected option magic {magic:#x}"
            )));
        }

        let option = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let len = u32::from_be_bytes(header[12..16].try_into().unwrap()) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                let export_name = parse_export_name(&payload)?;
                let export = match manager.get_export(&export_name).await {
                    Ok(export) => export,
                    Err(_) => {
                        stream
                            .write_all(&encode_option_reply(option, NBD_REP_ERR_UNKNOWN, &[]))
                            .await?;
                        continue;
                    }
                };
                send_export_legacy(stream, &export, client_flags).await?;
                return Ok(Some(export));
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                let go = option == NBD_OPT_GO;
                let request = parse_info_request(&payload)?;
                let export = match manager.get_export(&request.export_name).await {
                    Ok(export) => export,
                    Err(_) => {
                        stream
                            .write_all(&encode_option_reply(option, NBD_REP_ERR_UNKNOWN, &[]))
                            .await?;
                        continue;
                    }
                };

                for info in requested_infos(&request) {
                    let payload = encode_info_reply(
                        info,
                        ExportInfo {
                            image_size: export.image_size(),
                            export_name: export.export_name(),
                            export_description: &export.export_description(),
                        },
                    )?;
                    stream
                        .write_all(&encode_option_reply(option, NBD_REP_INFO, &payload))
                        .await?;
                }
                stream
                    .write_all(&encode_option_reply(option, NBD_REP_ACK, &[]))
                    .await?;
                if go {
                    return Ok(Some(export));
                }
            }
            NBD_OPT_ABORT => {
                stream
                    .write_all(&encode_option_reply(option, NBD_REP_ACK, &[]))
                    .await?;
                return Ok(None);
            }
            other => {
                stream
                    .write_all(&encode_option_reply(other, NBD_REP_ERR_UNSUP, &[]))
                    .await?;
            }
        }
    }
}

async fn transmission_phase(stream: TcpStream, export: Arc<Export>) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (reply_tx, mut reply_rx) = mpsc::channel::<Vec<u8>>(MAX_INFLIGHT_REQUESTS);
    let permits = Arc::new(Semaphore::new(MAX_INFLIGHT_REQUESTS));

    let writer_task = tokio::spawn(async move {
        while let Some(reply) = reply_rx.recv().await {
            writer.write_all(&reply).await?;
        }
        Ok::<(), Error>(())
    });

    loop {
        let mut header = [0_u8; 28];
        if let Err(error) = reader.read_exact(&mut header).await {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(Error::Io(error));
        }

        let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
        if magic != NBD_REQUEST_MAGIC {
            return Err(Error::InvalidRequest(format!(
                "unexpected request magic {magic:#x}"
            )));
        }

        let flags = u16::from_be_bytes(header[4..6].try_into().unwrap());
        let command = u16::from_be_bytes(header[6..8].try_into().unwrap());
        let handle = u64::from_be_bytes(header[8..16].try_into().unwrap());
        let offset = u64::from_be_bytes(header[16..24].try_into().unwrap());
        let len = u32::from_be_bytes(header[24..28].try_into().unwrap());

        match command {
            NBD_CMD_READ => {
                let permit =
                    permits.clone().acquire_owned().await.map_err(|_| {
                        Error::InvalidRequest("request semaphore closed".to_string())
                    })?;
                let export = export.clone();
                let reply_tx = reply_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let reply = match export.read(offset, len).await {
                        Ok(data) => encode_reply(handle, 0, Some(&data)),
                        Err(error) => {
                            tracing::warn!(offset, len, handle, "read request failed: {error}");
                            encode_reply(handle, libc::EIO as u32, None)
                        }
                    };
                    let _ = reply_tx.send(reply).await;
                });
            }
            NBD_CMD_WRITE => {
                let mut payload = vec![0_u8; len as usize];
                reader.read_exact(&mut payload).await?;
                let permit =
                    permits.clone().acquire_owned().await.map_err(|_| {
                        Error::InvalidRequest("request semaphore closed".to_string())
                    })?;
                let export = export.clone();
                let reply_tx = reply_tx.clone();
                let fua = flags & NBD_CMD_FLAG_FUA != 0;
                tokio::spawn(async move {
                    let _permit = permit;
                    let reply = match export.write(offset, &payload, fua).await {
                        Ok(()) => encode_reply(handle, 0, None),
                        Err(error) => {
                            tracing::warn!(offset, len, handle, "write request failed: {error}");
                            encode_reply(handle, libc::EIO as u32, None)
                        }
                    };
                    let _ = reply_tx.send(reply).await;
                });
            }
            NBD_CMD_FLUSH => {
                let permit =
                    permits.clone().acquire_owned().await.map_err(|_| {
                        Error::InvalidRequest("request semaphore closed".to_string())
                    })?;
                let export = export.clone();
                let reply_tx = reply_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let reply = match export.flush().await {
                        Ok(()) => encode_reply(handle, 0, None),
                        Err(error) => {
                            tracing::warn!(handle, "flush request failed: {error}");
                            encode_reply(handle, libc::EIO as u32, None)
                        }
                    };
                    let _ = reply_tx.send(reply).await;
                });
            }
            NBD_CMD_DISC => break,
            other => {
                reply_tx
                    .send(encode_reply(handle, libc::EINVAL as u32, None))
                    .await
                    .map_err(|_| Error::InvalidRequest("reply channel closed".to_string()))?;
                tracing::warn!("unsupported NBD command {other}");
            }
        }
    }

    drop(reply_tx);
    writer_task
        .await
        .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))??;
    Ok(())
}

async fn send_export_legacy(
    stream: &mut TcpStream,
    export: &Export,
    client_flags: u32,
) -> Result<()> {
    stream.write_all(&export.image_size().to_be_bytes()).await?;
    stream.write_all(&export_flags().to_be_bytes()).await?;
    if client_flags & u32::from(NBD_FLAG_NO_ZEROES) == 0 {
        stream.write_all(&[0_u8; 124]).await?;
    }
    Ok(())
}
