use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::error::{Error, Result};
use crate::export::Export;

const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943;
const NBD_IHAVEOPT: u64 = 0x4948_4156_454f_5054;
const NBD_REPLY_MAGIC: u32 = 0x6744_6698;
const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
const NBD_REP_MAGIC: u64 = 0x0003_e889_0455_65a9;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_FUA: u16 = 1 << 3;

const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

const NBD_REP_ACK: u32 = 1;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 1 << 31 | 1;
const NBD_REP_ERR_UNKNOWN: u32 = 1 << 31 | 6;

const NBD_INFO_EXPORT: u16 = 0;
const NBD_INFO_NAME: u16 = 1;
const NBD_INFO_DESCRIPTION: u16 = 2;
const NBD_INFO_BLOCK_SIZE: u16 = 3;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;

const NBD_CMD_FLAG_FUA: u16 = 1 << 0;

const MIN_BLOCK_SIZE: u32 = 1;
const PREFERRED_BLOCK_SIZE: u32 = 4096;
const MAX_BLOCK_SIZE: u32 = 32 * 1024 * 1024;

pub async fn serve_nbd(addr: std::net::SocketAddr, export: Arc<Export>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let export = export.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_client(stream, export).await {
                tracing::warn!("nbd session ended with error: {error}");
            }
        });
    }
}

async fn handle_client(mut stream: TcpStream, export: Arc<Export>) -> Result<()> {
    let client_flags = send_handshake(&mut stream).await?;
    if !negotiate_options(&mut stream, export.clone(), client_flags).await? {
        return Ok(());
    }
    transmission_phase(&mut stream, export).await
}

async fn send_handshake(stream: &mut TcpStream) -> Result<u32> {
    stream.write_all(&NBD_MAGIC.to_be_bytes()).await?;
    stream.write_all(&NBD_IHAVEOPT.to_be_bytes()).await?;
    stream
        .write_all(&(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES).to_be_bytes())
        .await?;

    let mut client_flags = [0_u8; 4];
    stream.read_exact(&mut client_flags).await?;
    Ok(u32::from_be_bytes(client_flags))
}

async fn negotiate_options(
    stream: &mut TcpStream,
    export: Arc<Export>,
    client_flags: u32,
) -> Result<bool> {
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
                validate_export_name(&payload, export.export_name())?;
                send_export_legacy(stream, &export, client_flags).await?;
                return Ok(true);
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                let go = option == NBD_OPT_GO;
                let request = parse_info_request(&payload)?;
                if request.export_name != export.export_name() {
                    send_option_reply(stream, option, NBD_REP_ERR_UNKNOWN, &[]).await?;
                    continue;
                }

                let infos = requested_infos(&request);
                for info in infos {
                    let payload = encode_info_reply(info, &export)?;
                    send_option_reply(stream, option, NBD_REP_INFO, &payload).await?;
                }
                send_option_reply(stream, option, NBD_REP_ACK, &[]).await?;
                if go {
                    return Ok(true);
                }
            }
            NBD_OPT_ABORT => {
                send_option_reply(stream, option, NBD_REP_ACK, &[]).await?;
                return Ok(false);
            }
            other => {
                send_option_reply(stream, other, NBD_REP_ERR_UNSUP, &[]).await?;
            }
        }
    }
}

async fn transmission_phase(stream: &mut TcpStream, export: Arc<Export>) -> Result<()> {
    loop {
        let mut header = [0_u8; 28];
        if let Err(error) = stream.read_exact(&mut header).await {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
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
                let data = export.read(offset, len).await?;
                send_reply(stream, handle, 0, Some(&data)).await?;
            }
            NBD_CMD_WRITE => {
                let mut payload = vec![0_u8; len as usize];
                stream.read_exact(&mut payload).await?;
                export
                    .write(offset, &payload, flags & NBD_CMD_FLAG_FUA != 0)
                    .await?;
                send_reply(stream, handle, 0, None).await?;
            }
            NBD_CMD_FLUSH => {
                export.flush().await?;
                send_reply(stream, handle, 0, None).await?;
            }
            NBD_CMD_DISC => return Ok(()),
            other => {
                send_reply(stream, handle, libc::EINVAL as u32, None).await?;
                return Err(Error::UnsupportedCommand(other));
            }
        }
    }
}

fn validate_export_name(payload: &[u8], expected: &str) -> Result<()> {
    let name = String::from_utf8(payload.to_vec())
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
    if name != expected {
        return Err(Error::InvalidRequest(format!(
            "unknown export {name}, expected {expected}"
        )));
    }
    Ok(())
}

fn encode_info_reply(info: u16, export: &Export) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&info.to_be_bytes());
    match info {
        NBD_INFO_EXPORT => {
            payload.extend_from_slice(&export.image_size().to_be_bytes());
            payload.extend_from_slice(&export_flags().to_be_bytes());
        }
        NBD_INFO_NAME => payload.extend_from_slice(export.export_name().as_bytes()),
        NBD_INFO_DESCRIPTION => payload.extend_from_slice(export.export_description().as_bytes()),
        NBD_INFO_BLOCK_SIZE => {
            payload.extend_from_slice(&MIN_BLOCK_SIZE.to_be_bytes());
            payload.extend_from_slice(&PREFERRED_BLOCK_SIZE.to_be_bytes());
            payload.extend_from_slice(&MAX_BLOCK_SIZE.to_be_bytes());
        }
        other => {
            return Err(Error::InvalidRequest(format!(
                "unsupported info request {other}"
            )));
        }
    }
    Ok(payload)
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

async fn send_option_reply(
    stream: &mut TcpStream,
    option: u32,
    reply_type: u32,
    payload: &[u8],
) -> Result<()> {
    stream.write_all(&NBD_REP_MAGIC.to_be_bytes()).await?;
    stream.write_all(&option.to_be_bytes()).await?;
    stream.write_all(&reply_type.to_be_bytes()).await?;
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    Ok(())
}

async fn send_reply(
    stream: &mut TcpStream,
    handle: u64,
    error: u32,
    payload: Option<&[u8]>,
) -> Result<()> {
    stream.write_all(&NBD_REPLY_MAGIC.to_be_bytes()).await?;
    stream.write_all(&error.to_be_bytes()).await?;
    stream.write_all(&handle.to_be_bytes()).await?;
    if let Some(payload) = payload {
        stream.write_all(payload).await?;
    }
    Ok(())
}

fn export_flags() -> u16 {
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA
}

struct InfoRequest {
    export_name: String,
    infos: BTreeSet<u16>,
}

fn parse_info_request(payload: &[u8]) -> Result<InfoRequest> {
    if payload.len() < 6 {
        return Err(Error::InvalidRequest(
            "info/go payload too short".to_string(),
        ));
    }

    let name_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    if payload.len() < 4 + name_len + 2 {
        return Err(Error::InvalidRequest(
            "info/go payload truncated".to_string(),
        ));
    }

    let name = String::from_utf8(payload[4..4 + name_len].to_vec())
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
    let info_count = u16::from_be_bytes(payload[4 + name_len..6 + name_len].try_into().unwrap());
    let infos_start = 6 + name_len;
    let expected_len = infos_start + info_count as usize * 2;
    if expected_len != payload.len() {
        return Err(Error::InvalidRequest(format!(
            "info/go payload length mismatch: expected {expected_len}, got {}",
            payload.len()
        )));
    }

    let mut infos = BTreeSet::new();
    for index in 0..info_count as usize {
        let start = infos_start + index * 2;
        infos.insert(u16::from_be_bytes(
            payload[start..start + 2].try_into().unwrap(),
        ));
    }

    Ok(InfoRequest {
        export_name: name,
        infos,
    })
}

fn requested_infos(request: &InfoRequest) -> Vec<u16> {
    if request.infos.is_empty() {
        return vec![
            NBD_INFO_EXPORT,
            NBD_INFO_NAME,
            NBD_INFO_DESCRIPTION,
            NBD_INFO_BLOCK_SIZE,
        ];
    }

    let mut infos = vec![NBD_INFO_EXPORT];
    for info in &request.infos {
        if matches!(
            *info,
            NBD_INFO_NAME | NBD_INFO_DESCRIPTION | NBD_INFO_BLOCK_SIZE
        ) && *info != NBD_INFO_EXPORT
        {
            infos.push(*info);
        }
    }
    infos.sort_unstable();
    infos.dedup();
    infos
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        InfoRequest, NBD_INFO_BLOCK_SIZE, NBD_INFO_DESCRIPTION, NBD_INFO_EXPORT, NBD_INFO_NAME,
        parse_info_request, requested_infos,
    };

    #[test]
    fn parse_go_payload() {
        let payload = [
            0, 0, 0, 6, b'e', b'x', b'p', b'o', b'r', b't', 0, 2, 0, 0, 0, 3,
        ];
        let request = parse_info_request(&payload).unwrap();
        assert_eq!(request.export_name, "export");
        assert!(request.infos.contains(&NBD_INFO_EXPORT));
        assert!(request.infos.contains(&NBD_INFO_BLOCK_SIZE));
    }

    #[test]
    fn default_info_set_is_export_name_description_block_size() {
        let request = InfoRequest {
            export_name: "export".to_string(),
            infos: BTreeSet::new(),
        };
        assert_eq!(
            requested_infos(&request),
            vec![
                NBD_INFO_EXPORT,
                NBD_INFO_NAME,
                NBD_INFO_DESCRIPTION,
                NBD_INFO_BLOCK_SIZE
            ]
        );
    }
}
