use std::sync::{Arc, Mutex};

use s3::{creds::Credentials, Bucket};
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

fn do_downloads(num_workers: u32, samples: u32, bucket: &str, key_prefix: &str, region: &str) {
    let to_do = Arc::new(Mutex::new(samples));
    let mut jobs = vec![];
    let start = std::time::Instant::now();
    let bucket = Arc::new(
        Bucket::new(
            bucket,
            region.parse().unwrap(),
            Credentials::default().unwrap(),
        )
        .unwrap(),
    );
    for i in 0..num_workers {
        let to_do = Arc::clone(&to_do);
        let bucket = Arc::clone(&bucket);
        let key = format!("{}-{}", key_prefix, i % 40);
        let job = std::thread::spawn(move || {
            let mut worker_bytes = 0;
            loop {
                if take_job(to_do.as_ref()) {
                    let (data, _) = bucket.get_object_blocking(&key).unwrap();
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
        "{:?}, {} MB/s",
        time,
        total_bytes as f64 / (time.as_secs_f64() * 1024.0 * 1024.0)
    );
}

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "start-num-workers")]
    start_num_workers: u32,
    #[structopt(long = "samples")]
    samples: u32,
    #[structopt(long = "bucket", default_value = "abk-test-rusoto-download")]
    bucket: String,
    #[structopt(long = "key-prefix", default_value = "test-object")]
    key_prefix: String,
    #[structopt(long = "region", default_value = "us-east-1")]
    region: String,
}

fn main() {
    let Args {
        start_num_workers,
        samples,
        bucket,
        key_prefix,
        region,
    } = Args::from_args();
    println!("Time, Throughput");
    for num_workers in (start_num_workers..).step_by(5) {
        do_downloads(num_workers, samples, &bucket, &key_prefix, &region)
    }
}
