use std::{
    fs::File,
    io::{self, Seek, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    routing::get,
    Router,
};
use clap::Parser;
use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use tap::Tap;
use tokio::fs::{self};
use tower_http::trace::TraceLayer;
use tqdm::tqdm;

#[derive(Debug, Parser)]
#[command()]
enum Command {
    Serve,

    Shuffle {
        #[arg(long)]
        output_data_raw: PathBuf,

        #[arg(long)]
        output_metadata: PathBuf,
    },
}

#[derive(Debug, Parser)]
#[command()]
struct Args {
    #[arg(short, long)]
    metadata: PathBuf,

    #[arg(short, long)]
    data: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Serialize, Deserialize, Debug)]
struct PartOffset {
    offset: u64,
    length: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Metadata<Part> {
    url: String,
    likes: u64,
    parts: Vec<Part>,
    rating: String,
    direction: String,
    category: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Metadatas<Part>(Vec<Metadata<Part>>);

impl<Part> Metadatas<Part> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

struct ServerStateInner {
    metadata: Metadatas<PartOffset>,
    raw: File,
}

impl ServerStateInner {
    fn get_subseg(&self, start: usize, len: usize) -> io::Result<Metadatas<String>> {
        let metas = self.get_meta_subseg(start, len);
        let mut new_metas = Vec::with_capacity(metas.len());

        for m in metas {
            let mut parts = Vec::with_capacity(m.parts.len());

            for p in &m.parts {
                let mut part = vec![0u8; p.length];
                self.raw.read_exact_at(&mut part, p.offset)?;
                parts.push(
                    String::from_utf8(part)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                );
            }

            new_metas.push(Metadata {
                url: m.url.clone(),
                likes: m.likes,
                parts,
                rating: m.rating.clone(),
                direction: m.direction.clone(),
                category: m.category.clone(),
            });
        }

        Ok(Metadatas(new_metas))
    }

    fn get_meta_subseg(&self, start: usize, len: usize) -> &[Metadata<PartOffset>] {
        if start >= self.metadata.len() {
            return &[];
        }
        let end = (start + len).min(self.metadata.len());
        &self.metadata.0[start..end]
    }
}

#[derive(Clone)]
struct ServerState(Arc<ServerStateInner>);

impl ServerState {
    fn get_subseg(&self, start: usize, len: usize) -> io::Result<Metadatas<String>> {
        self.0.get_subseg(start, len)
    }

    fn get_meta_subseg(&self, start: usize, len: usize) -> &[Metadata<PartOffset>] {
        self.0.get_meta_subseg(start, len)
    }
}

#[derive(Debug, Deserialize)]
struct Page {
    start: usize,
    count: usize,
}

async fn handle_paged_get(
    state: State<ServerState>,
    Query(Page { start, count }): Query<Page>,
) -> Result<axum::Json<Metadatas<String>>, (axum::http::StatusCode, &'static str)> {
    if count > 10000 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "count is greated than 10000",
        ));
    }
    state
        .get_subseg(start, count)
        .map_err(|e| {
            tracing::error!("Failed to read fanfics: {e}");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
            )
        })
        .map(axum::Json)
}

async fn handle_metadata_get(
    state: State<ServerState>,
    Query(Page { start, count }): Query<Page>,
) -> Result<axum::Json<Metadatas<usize>>, (axum::http::StatusCode, &'static str)> {
    if count > 100000 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "count is greated than 100000",
        ));
    }

    let metas = state.get_meta_subseg(start, count);
    let mut new_metas = Vec::with_capacity(metas.len());

    for m in metas {
        new_metas.push(Metadata {
            url: m.url.clone(),
            category: m.category.clone(),
            direction: m.direction.clone(),
            likes: m.likes,
            parts: m.parts.iter().map(|p| p.length).collect(),
            rating: m.rating.clone(),
        });
    }

    Ok(axum::Json(Metadatas(new_metas)))
}

async fn serve(meta: Metadatas<PartOffset>, raw: File) {
    let state = ServerState(Arc::new(ServerStateInner {
        metadata: meta,
        raw,
    }));
    let app = Router::new()
        .route("/fanfic", get(handle_paged_get))
        .route("/metadata", get(handle_metadata_get))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listen = tokio::net::TcpListener::bind("0.0.0.0:1235").await.unwrap();

    axum::serve(listen, app).await.unwrap();
}

fn shuffle(
    mut meta: Metadatas<PartOffset>,
    raw: File,
    output_metadata: PathBuf,
    output_data_raw: PathBuf,
) -> io::Result<()> {
    let mut output_raw = File::options()
        .tap_mut(|o| {
            o.write(true).create(true);
        })
        .open(output_data_raw)?;
    meta.0.shuffle(&mut thread_rng());

    for m in tqdm(&mut meta.0) {
        for p in &mut m.parts {
            let mut b = vec![0; p.length];
            raw.read_exact_at(&mut b, p.offset)?;
            p.offset = output_raw.stream_position()?;
            output_raw.write_all(&b)?;
            String::from_utf8(b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
    }
    output_raw.flush()?;

    let mut new_meta = File::options()
        .tap_mut(|o| {
            o.write(true).create(true);
        })
        .open(output_metadata)?;
    serde_json::to_writer(&mut new_meta, &meta)?;
    new_meta.flush()?;

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .with_level(true)
        .init();

    let Args {
        metadata,
        data,
        command,
    } = Args::parse();
    tracing::info!("Parsed args");

    let metadata = fs::read_to_string(&metadata).await.unwrap();
    let metadata: Metadatas<PartOffset> = serde_json::from_str(&metadata).unwrap();
    tracing::info!("Parsed metadata");

    let raw = std::fs::File::open(&data).unwrap();
    tracing::info!("Opened fanfics file");

    match command {
        Command::Serve => serve(metadata, raw).await,
        Command::Shuffle {
            output_data_raw,
            output_metadata,
        } => shuffle(metadata, raw, output_metadata, output_data_raw).unwrap(),
    }
}
