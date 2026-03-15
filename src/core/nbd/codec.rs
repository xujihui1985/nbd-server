use std::collections::BTreeSet;

use crate::core::error::{Error, Result};
use crate::core::nbd::protocol::{
    MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, NBD_INFO_BLOCK_SIZE, NBD_INFO_DESCRIPTION, NBD_INFO_EXPORT,
    NBD_INFO_NAME, NBD_REP_MAGIC, NBD_REPLY_MAGIC, PREFERRED_BLOCK_SIZE, export_flags,
};

pub struct ExportInfo<'a> {
    pub image_size: u64,
    pub export_name: &'a str,
    pub export_description: &'a str,
}

pub struct InfoRequest {
    pub export_name: String,
    pub infos: BTreeSet<u16>,
}

pub fn parse_export_name(payload: &[u8]) -> Result<String> {
    String::from_utf8(payload.to_vec()).map_err(|error| Error::InvalidRequest(error.to_string()))
}

pub fn parse_info_request(payload: &[u8]) -> Result<InfoRequest> {
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

pub fn requested_infos(request: &InfoRequest) -> Vec<u16> {
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

pub fn encode_info_reply(info: u16, export: ExportInfo<'_>) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&info.to_be_bytes());
    match info {
        NBD_INFO_EXPORT => {
            payload.extend_from_slice(&export.image_size.to_be_bytes());
            payload.extend_from_slice(&export_flags().to_be_bytes());
        }
        NBD_INFO_NAME => payload.extend_from_slice(export.export_name.as_bytes()),
        NBD_INFO_DESCRIPTION => payload.extend_from_slice(export.export_description.as_bytes()),
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

pub fn encode_reply(handle: u64, error: u32, payload: Option<&[u8]>) -> Vec<u8> {
    let payload_len = payload.map_or(0, <[u8]>::len);
    let mut reply = Vec::with_capacity(16 + payload_len);
    reply.extend_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
    reply.extend_from_slice(&error.to_be_bytes());
    reply.extend_from_slice(&handle.to_be_bytes());
    if let Some(payload) = payload {
        reply.extend_from_slice(payload);
    }
    reply
}

pub fn encode_option_reply(option: u32, reply_type: u32, payload: &[u8]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(20 + payload.len());
    reply.extend_from_slice(&NBD_REP_MAGIC.to_be_bytes());
    reply.extend_from_slice(&option.to_be_bytes());
    reply.extend_from_slice(&reply_type.to_be_bytes());
    reply.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    reply.extend_from_slice(payload);
    reply
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        ExportInfo, InfoRequest, encode_info_reply, encode_reply, parse_info_request,
        requested_infos,
    };
    use crate::core::nbd::protocol::{
        NBD_INFO_BLOCK_SIZE, NBD_INFO_DESCRIPTION, NBD_INFO_EXPORT, NBD_INFO_NAME, NBD_REPLY_MAGIC,
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

    #[test]
    fn encode_reply_formats_header_and_payload() {
        let payload = [1_u8, 2, 3, 4];
        let reply = encode_reply(7, 5, Some(&payload));
        assert_eq!(&reply[0..4], &NBD_REPLY_MAGIC.to_be_bytes());
        assert_eq!(&reply[4..8], &5_u32.to_be_bytes());
        assert_eq!(&reply[8..16], &7_u64.to_be_bytes());
        assert_eq!(&reply[16..], &payload);
    }

    #[test]
    fn encode_info_reply_formats_export_payload() {
        let payload = encode_info_reply(
            NBD_INFO_EXPORT,
            ExportInfo {
                image_size: 4096,
                export_name: "export",
                export_description: "desc",
            },
        )
        .unwrap();
        assert_eq!(&payload[0..2], &NBD_INFO_EXPORT.to_be_bytes());
    }
}
