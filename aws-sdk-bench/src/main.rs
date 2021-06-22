use std::error::Error;
use std::sync::{Arc, Mutex};

extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use aws_sdk_s3 as s3;
use s3::Region;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::SubscriberBuilder;

use structopt::StructOpt;

fn take_job(to_do: &Mutex<u32>) -> bool {
    let mut jobs_left = to_do.lock().unwrap();
    if *jobs_left <= 0 {
        false
    } else {
        *jobs_left -= 1;
        true
    }
}

async fn do_downloads(num_workers: u32, samples: u32, bucket: &str, key_prefix: &str, region: &str) -> Option<&str> {
    let to_do = Arc::new(Mutex::new(samples));
    let mut jobs = vec![];
    let start = std::time::Instant::now();

    // XXX: Logs/tracer might affect perf against the other two impls?
    SubscriberBuilder::default()
        .with_env_filter("info")
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let conf = s3::Config::builder()
        .region(Region::new(region))
        .build();

    // XXX: Does the client need to be within an Arc?
    // XXX: Is it fair to compare blocking impls like the rust-s3 one with this async? Perhaps should implement rust-s3 async too.
    let client = s3::Client::from_conf(conf);

    for i in 0..num_workers {
        let to_do = Arc::clone(&to_do);
        let key = format!("{}-{}", key_prefix, i % 40);
        let job = std::thread::spawn(move || {
            let mut worker_bytes = 0;
            loop {
                if take_job(to_do.as_ref()) {
                    let resp = client.get_object().bucket(bucket).key(key_prefix).send().await?;
                    let data = resp.body.collect().await?;

                    assert_eq!(code, 200);
                    worker_bytes += data.len();
                } else {
                    break;
                }
            }
            worker_bytes
        });
        jobs.push(job)
    }

    let total_bytes = jobs
        .into_iter()
        .fold(0, |accum, job| accum + job.join().unwrap());
    let time = start.elapsed();
    println!(
        "{:?}, {}, {} MB/s",
        time,
        total_bytes,
        total_bytes as f64 / (time.as_secs_f64() * 1024.0 * 1024.0)
    );

    Some("fine")
}

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "workers")]
    workers: u32,
    #[structopt(long = "samples")]
    samples: u32,
    #[structopt(long = "bucket", default_value = "abk-test-rusoto-download")]
    bucket: String,
    #[structopt(long = "key-prefix", default_value = "test-object-8388608")]
    key_prefix: String,
    #[structopt(long = "region", default_value = "us-east-1")]
    region: String,
}

fn main() {
    let Args {
        workers,
        samples,
        bucket,
        key_prefix,
        region,
    } = Args::from_args();
    println!("Time, Total Bytes, Throughput");
    do_downloads(workers, samples, &bucket, &key_prefix, &region);
}
