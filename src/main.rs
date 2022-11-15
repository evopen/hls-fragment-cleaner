//! delete all unreferenced ts fragment
//!
//! criterias for ts deletion,
//!
//! scenario 1:
//! * ts corresponding playlist file must exist
//! * ts is not referenced in that playlist
//! * ts sequence number must be smaller than any other referenced sequence number in that playlist
//!
//! scenario 2:
//! * ts does not have corresponding playlist file
//! * ts file is older than 30 minutes

use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::Context;
use tracing::{instrument, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

const HLS_DIR: &str = "/tmp/hls";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    tracing::info!("ts cleaner initialized");
    run().await
}

async fn run() -> anyhow::Result<()> {
    let Ok(cleanup) = std::env::var("HLS_CLEANUP") else {
        tracing::info!("HLS_CLEANUP is not set, exiting");
        return Ok(());
    } ;
    if cleanup != "off" {
        tracing::info!("cleanup is done by nginx process, exiting");
        return Ok(());
    }
    println!("launching cleanup process");

    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        tracing::trace!("launching task");
        if let Err(e) = tokio::spawn(clean_task()).await? {
            tracing::error!("{}", e);
        }
    }
}

#[instrument(level = "trace")]
async fn clean_task() -> anyhow::Result<()> {
    let ts_matcher = globset::GlobBuilder::new("*.ts").build()?.compile_matcher();
    let current_time = std::time::SystemTime::now();
    for ts_entry in walkdir::WalkDir::new(HLS_DIR)
        .min_depth(1)
        .max_depth(1)
        .contents_first(true)
        .into_iter()
        .filter_entry(|e| ts_matcher.is_match(e.path()) && e.file_type().is_file())
        .filter_map(|e| e.ok())
    {
        tracing::debug!("processing {}", ts_entry.path().display());
        let file_stem = ts_entry
            .path()
            .file_stem()
            .with_context(|| format!("{} has not file stem", ts_entry.path().display()))?
            .to_str()
            .with_context(|| format!("{} contains invalid character", ts_entry.path().display()))?;
        let (stream_base_name, sequence_num) = file_stem
            .rsplit_once('-')
            .map(|(base, num)| {
                (
                    base,
                    num.parse::<u32>()
                        .with_context(|| format!("invalid sequence num {}", num)),
                )
            })
            .with_context(|| file_stem.to_owned())?;
        let sequence_num = sequence_num?;
        let playlist_path = ts_entry
            .path()
            .parent()
            .with_context(|| format!("{} does not have a parent", ts_entry.path().display()))?
            .join(format!("{}.m3u8", stream_base_name));
        match playlist_path.exists() {
            true => {
                tracing::trace!("playlist {} exist", playlist_path.display());
                let playlist_content = std::fs::read_to_string(&playlist_path)
                    .with_context(|| format!("{}", playlist_path.display()))?;
                let playlist = hls_m3u8::MediaPlaylist::from_str(&playlist_content)
                    .with_context(|| playlist_content.to_string())?;
                let segment_paths = playlist
                    .segments
                    .iter()
                    .map(|(_, seg)| {
                        PathBuf::from_str(seg.uri())
                            .with_context(|| format!("invalid path {}", seg.uri()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let file_stems = segment_paths
                    .iter()
                    .map(|p| {
                        p.file_stem()
                            .with_context(|| format!("{} does not have stem", p.display()))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|stem| {
                        stem.to_str().with_context(|| {
                            format!("path {} contains invalid character", stem.to_string_lossy())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let min_sequence_num = file_stems
                    .into_iter()
                    .map(|s| {
                        s.split_once('-')
                            .with_context(|| format!("invalid segment name {}", s))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .iter()
                    .map(|split| split.1.parse::<u32>())
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .min()
                    .with_context(|| format!("{} has no segments", playlist_path.display()))?;
                if sequence_num < min_sequence_num {
                    tracing::trace!("{} is not in playlist, deleting", ts_entry.path().display());
                    if let Err(e) = std::fs::remove_file(ts_entry.path()) {
                        tracing::warn!("unable to remove {} - {}", ts_entry.path().display(), e);
                    }
                }
            }
            false => {
                tracing::trace!("playlist {} does not exist", playlist_path.display());

                match tokio::fs::metadata(ts_entry.path()).await {
                    Ok(metadata) => match metadata.accessed() {
                        Ok(time) => {
                            if let Ok(duration_since_access) = current_time.duration_since(time) {
                                if duration_since_access > std::time::Duration::from_secs(1800) {
                                    tracing::trace!(
                                        "{} older than 30 minutes, deleting",
                                        ts_entry.path().display()
                                    );
                                    if let Err(e) = std::fs::remove_file(ts_entry.path()) {
                                        tracing::error!(
                                            "unable to remove {} - {}",
                                            ts_entry.path().display(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "error reading access time for {} - {}",
                                ts_entry.path().display(),
                                e
                            )
                        }
                    },
                    Err(e) => tracing::error!(
                        "error getting metadata for {} - {}",
                        ts_entry.path().display(),
                        e
                    ),
                }
            }
        }
    }
    Ok(())
}
