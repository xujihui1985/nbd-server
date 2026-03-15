use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CloneSource {
    pub export_id: String,
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExportSpec {
    pub export_id: String,
    pub cache_dir: PathBuf,
    pub chunk_size: u64,
    pub snapshot_id: Option<String>,
    pub image_size: Option<u64>,
    pub clone_source: Option<CloneSource>,
}
