use clap::Parser;
use crossbeam_channel::tick;
use log::*;
use nats::jetstream::StorageType;
use nats::kv::Config;
use nats::{jetstream::JetStream, kv::Store};
use serde::{Deserialize, Serialize};
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, env)]
    bucket_config_path: String,

    #[clap(long, env)]
    nats_url: String,

    #[clap(long, env, default_value_t = 1800)]
    test_duration_secs: u64,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let exit = Arc::new(AtomicBool::new(false));
    let store = setup(&args);

    let reader_thread = spawn_reader(store.clone(), exit.clone());
    let writer_thread = spawn_writer(store, exit.clone());

    sleep(Duration::from_secs(args.test_duration_secs));

    info!("exiting...");
    exit.store(true, Ordering::Relaxed);

    reader_thread.join().unwrap();
    writer_thread.join().unwrap();
}

/// Writes to values to the KV store, then continuously updates one KV pair
/// to prevent it from being purged due to TTL, while letting the other TTL.
fn spawn_writer(store: Arc<Store>, exit: Arc<AtomicBool>) -> JoinHandle<()> {
    Builder::new()
        .name("writer-thread".to_string())
        .spawn(move || {
            let (key_0, value_0): (&str, &str) = ("key-0", "val-0");
            let (key_1, value_1): (&str, &str) = ("key-1", "val-1");

            store.put(key_0, value_0).unwrap();
            store.put(key_1, value_1).unwrap();

            let tick = tick(Duration::from_millis(800));
            for _tick in tick.iter() {
                if exit.load(Ordering::Relaxed) {
                    info!("writer thread exiting...");
                    break;
                }
                store.put(key_0, value_0).unwrap();
            }
        })
        .unwrap()
}

/// Calls `keys` on the KV store every 2 seconds.
fn spawn_reader(store: Arc<Store>, exit: Arc<AtomicBool>) -> JoinHandle<()> {
    Builder::new()
        .name("reader-thread".to_string())
        .spawn(move || {
            let tick = tick(Duration::from_secs(2));
            for _tick in tick.iter() {
                if exit.load(Ordering::Relaxed) {
                    info!("reader thread exiting...");
                    break;
                }

                let keys: Vec<_> = store
                    .keys()
                    .unwrap()
                    .into_iter()
                    .map(Into::<String>::into)
                    .collect();
                info!("found {} values", keys.len());
            }
        })
        .unwrap()
}

fn setup(args: &Args) -> Arc<Store> {
    let cfg = read_bucket_config(
        args.bucket_config_path.to_string(),
        "test-bucket".to_string(),
    );
    let store = create_bucket(&args.nats_url, cfg, true).unwrap().unwrap();

    Arc::new(store)
}

fn read_bucket_config(path: String, expected_bucket_name: String) -> JetStreamBucketConfig {
    let f = std::fs::File::open(path).unwrap();
    let config: JetStreamBucketConfig = serde_yaml::from_reader(f).unwrap();
    assert_eq!(config.bucket, expected_bucket_name);

    config
}

/// Create a JetStream bucket.
fn create_bucket(
    nats_url: &str,
    cfg: JetStreamBucketConfig,
    overwrite_existing: bool,
) -> io::Result<Option<Store>> {
    let conn = nats::connect(nats_url).expect("failed to connect to nats");
    let js = JetStream::new(conn, Default::default());

    let should_create = if overwrite_existing {
        // delete if it already exists
        if let Ok(store) = js.key_value(&cfg.bucket) {
            js.delete_key_value(store.bucket())?;
        }
        true
    } else {
        // create only if it DNE
        js.key_value(&cfg.bucket).is_err()
    };

    let maybe_store = if should_create {
        Some(js.create_key_value(&cfg.into())?)
    } else {
        None
    };

    Ok(maybe_store)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JetStreamBucketConfig {
    /// Name of the bucket
    pub bucket: String,
    /// Human readable description.
    pub description: String,
    /// Maximum size of a single value.
    pub max_value_size: i32,
    /// Maximum historical entries.
    pub history: i64,
    /// Maximum age of any entry in the bucket in milliseconds
    pub max_age_ms: u64,
    /// How large the bucket may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// How many replicas to keep for each entry in a cluster.
    pub num_replicas: usize,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage_type: u8,
}

impl From<JetStreamBucketConfig> for Config {
    fn from(from: JetStreamBucketConfig) -> Config {
        Config {
            bucket: from.bucket,
            description: from.description,
            max_value_size: from.max_value_size,
            history: from.history,
            max_age: Duration::from_millis(from.max_age_ms),
            max_bytes: from.max_bytes,
            storage: {
                match from.storage_type {
                    0 => StorageType::File,
                    1 => StorageType::Memory,
                    _ => panic!(
                        "Invalid `storage_type` value. Valid values are 0 for File or 1 for Memory"
                    ),
                }
            },
            num_replicas: from.num_replicas,
        }
    }
}
