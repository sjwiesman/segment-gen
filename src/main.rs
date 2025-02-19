use std::collections::HashMap;
use chrono::Utc;
use clap::Parser;
use env_logger;
use log::{error, info, warn};
use num_cpus;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use serde::Serialize;
use signal_hook::{consts::SIGTERM, iterator::Signals};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct Context {
    user_agent: &'static str,
}

#[derive(Serialize)]
struct VideoSecondsViewed {
    video_seconds_viewed: i64,
}

#[derive(Serialize)]
struct Body {
    received_at: String,
    sent_at: String,
    timestamp: String,
}

#[derive(Serialize)]
struct Properties {
    video_series_name: &'static str,
    program: &'static str,
    video_asset_title: &'static str,
    title: &'static str,
}

#[derive(Serialize)]
struct SegmentEvent {
    anonymous_id: &'static str,
    channel: &'static str,
    video_seconds_viewed: VideoSecondsViewed,
    context: Context,
    body: Body,
    properties: Properties,
    event: &'static str,
    #[serde(flatten)]
    buffer: &'static HashMap<String, String>,
}

struct RateLimiter {
    start: Instant,
    interval: Duration,
}

impl RateLimiter {
    fn new(interval: Duration) -> Self {
        Self {
            start: Instant::now(),
            interval,
        }
    }

    fn sleep(&mut self) -> Option<Duration> {
        let elapsed = self.start.elapsed();
        let result = if elapsed < self.interval {
            let duration = self.interval - elapsed;
            thread::sleep(duration);
            Some(duration)
        } else {
            None
        };

        self.start = Instant::now();
        result
    }
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    username: String,

    #[clap(short, long)]
    password: String,

    #[clap(short, long)]
    brokers: String,

    #[clap(short, long)]
    topic: String,

    #[clap(long, default_value_t = 1000)]
    buffer_size: usize,

    #[clap(short, long, default_value_t = 1024000)]
    num_elems_per_second: usize,
}

#[derive(Clone)]
struct Pool {
    vec: Vec<&'static str>,
    index: usize,
}

impl Pool {
    fn get(&mut self) -> &'static str {
        let result = self.vec[self.index];
        self.index = (self.index + 1) % self.vec.len();
        result
    }
}

fn get_random_strings() -> Pool {
    Pool {
        vec: (1000000..9999999)
            .map(|i| format!("{:x}{:x}", i, 1))
            .map(|s| {
                let immutable_str: &str = Box::leak(s.into_boxed_str());
                immutable_str
            })
            .collect(),
        index: 0,
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

fn random_string(size: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const CHARSET_LEN: usize = CHARSET.len();

    let mut result = String::with_capacity(size);

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let mut seed = since_the_epoch.as_secs() as usize;

    for _ in 0..size {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = seed % CHARSET_LEN;
        result.push(CHARSET[idx] as char);
    }

    result
}

fn generate_map(total_size: usize) -> &'static HashMap<String, String> {
    let mut map = HashMap::new();
    let mut current_size = 0;

    while current_size < total_size {
        // Assume average key and value size
        let key_size = 8;
        let value_size = 16;
        let key = random_string(key_size);
        let value = random_string(value_size);

        current_size += key_size + value_size;
        map.insert(key, value);
    }

    Box::leak(Box::new(map))
}

fn get_buffer(size: usize) -> &'static str {
    let mut result = String::with_capacity(size);
    let mut current_size = 0;

    while current_size < size {
        result.push('0');
        current_size += '0'.len_utf8();
    }

    Box::leak(result.into_boxed_str())
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();
    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "PLAIN")
        .set("bootstrap.servers", args.brokers)
        .set("sasl.username", args.username)
        .set("sasl.password", args.password)
        .set("queue.buffering.max.messages", "1000000")
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("queue.buffering.max.ms", "50")
        .set("linger.ms", "50")
        .set("compression.type", "lz4")
        .set("acks", "1")
        .create()
        .expect("Producer creation error");

    let producer = Arc::new(producer);

    let shutdown_flag = Arc::new(Mutex::new(false));
    let shutdown_flag_clone = shutdown_flag.clone();

    let num_cores = num_cpus::get();
    let (num_threads, num_elems) = if args.num_elems_per_second < num_cores  {
        (args.num_elems_per_second, 1)
    } else {
        (num_cores, args.num_elems_per_second / num_cores)
    };

    println!("generating random data");
    let random_strings = get_random_strings();
    let buffer = generate_map(args.buffer_size);

    println!("producing {} elems per second", args.num_elems_per_second);
    println!("spawning {num_threads} threads");

    for core_id in 0..num_threads {
        let producer = producer.clone();
        let shutdown_flag = shutdown_flag.clone();
        let topic = args.topic.clone();
        let mut random_strings = random_strings.clone();

        thread::spawn(move || {
            let mut rate_limiter = RateLimiter::new(Duration::from_secs(1));

            let mut avg_size = if core_id == 0 {
                Some(0)
            } else {
                None
            };

            loop {
                let now = Utc::now().to_rfc3339();
                if *shutdown_flag.lock().unwrap() {
                    info!("Thread {} received shutdown signal", core_id);
                    break;
                }

                for i in 0..num_elems {
                    let event = SegmentEvent {
                        anonymous_id: random_strings.get(),
                        channel: random_strings.get(),
                        video_seconds_viewed: VideoSecondsViewed {
                            video_seconds_viewed: (i % 1000) as i64,
                        },
                        context: Context {
                            user_agent: random_strings.get(),
                        },
                        body: Body {
                            received_at: now.clone(),
                            sent_at: now.clone(),
                            timestamp: now.clone(),
                        },
                        properties: Properties {
                            video_series_name: random_strings.get(),
                            program: random_strings.get(),
                            video_asset_title: random_strings.get(),
                            title: random_strings.get(),
                        },
                        event: random_strings.get(),
                        buffer,
                    };

                    let payload = match serde_json::to_string(&event) {
                        Ok(payload) => payload,
                        Err(e) => {
                            error!("Serialization error: {}", e);
                            continue;
                        }
                    };

                    if let Some(ref mut value) = avg_size {
                        *value += payload.len()
                    }

                    if let Err((e, _)) =
                        producer.send::<String, String>(BaseRecord::to(&topic).payload(&payload))
                    {
                        if matches!(
                            e,
                            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        ) {
                            let _ = producer.flush(Duration::from_secs(10));
                        } else {
                            panic!("{:?}", e);
                        }
                    }
                }

                if let Some(sum) = avg_size.take() {
                    let avg = sum / num_elems;
                    info!("average payload size is {} bytes", avg)
                }

                if let Some(duration) = rate_limiter.sleep() {
                    warn!(
                        "Thread {} is falling behind. Processing took {:?} ms.",
                        core_id,
                        duration.as_millis()
                    );
                }
            }
        });
    }

    // Handle graceful shutdown
    let mut signals = Signals::new(&[SIGTERM]).expect("Failed to create signal handler");
    for _ in signals.forever() {
        let mut shutdown_flag = shutdown_flag_clone.lock().unwrap();
        *shutdown_flag = true;
        break;
    }

    // Prevent the main thread from exiting until all threads have finished
    loop {
        thread::park();
    }
}
