use std::sync::Arc;

use rusoto_core::{ByteStream, Region};
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use structopt::StructOpt;
use tokio::{io::AsyncReadExt, sync::Mutex};

pub async fn create_files(
    file_size: usize,
    num_files: u64,
    region: Region,
    bucket: &str,
    key_prefix: &str,
) {
    let client = S3Client::new(region);
    for i in 0..num_files {
        let key = format!("{}-{}", key_prefix, i);
        let data = vec![0; file_size];
        let request = PutObjectRequest {
            key,
            body: Some(ByteStream::from(data)),
            bucket: bucket.to_string(),
            content_length: Some(file_size as i64),
            ..Default::default()
        };
        client
            .put_object(request)
            .await
            .expect("Error uploading file");
    }
}

async fn take_job(to_do: &Mutex<u32>) -> bool {
    let mut jobs_left = to_do.lock().await;
    if *jobs_left <= 0 {
        false
    } else {
        *jobs_left -= 1;
        true
    }
}

pub async fn download_test(
    num_workers: u64,
    samples: u32,
    region: Region,
    bucket: &str,
    key_prefix: &str,
) {
    let client = Arc::new(S3Client::new(region));

    let start = std::time::Instant::now();
    let to_do = Arc::new(Mutex::new(samples));
    let mut tasks = vec![];
    for i in 0..num_workers {
        let client_copy = Arc::clone(&client);
        let to_do_copy = Arc::clone(&to_do);
        let key = format!("{}-{}", key_prefix, i % 40);
        let bucket = bucket.to_string();
        let task = tokio::task::spawn(async move {
            let mut worker_first = std::time::Duration::new(0, 0);
            let mut worker_last = std::time::Duration::new(0, 0);
            let mut worker_bytes = 0;
            loop {
                if take_job(to_do_copy.as_ref()).await {
                    let start = tokio::time::Instant::now();
                    let request = GetObjectRequest {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                        ..Default::default()
                    };
                    let response = client_copy
                        .get_object(request)
                        .await
                        .expect("Error getting object");
                    worker_first += start.elapsed();
                    let body = response.body.unwrap();
                    let mut reader = body.into_async_read();
                    let mut buf = Vec::new();
                    reader
                        .read_to_end(&mut buf)
                        .await
                        .expect("Error writing to buffer");
                    worker_last += start.elapsed();
                    worker_bytes += buf.len()
                } else {
                    break;
                }
            }
            (worker_first, worker_last, worker_bytes)
        });
        tasks.push(task);
    }
    let mut total_first = std::time::Duration::new(0, 0);
    let mut total_last = std::time::Duration::new(0, 0);
    let mut total_bytes = 0;
    for task in tasks {
        let (first, last, bytes) = task.await.unwrap();
        total_first += first;
        total_last += last;
        total_bytes += bytes;
    }
    let time = start.elapsed();
    println!(
        "{:?}, {} MB, {} MB/s, {:?}, {:?}",
        time,
        total_bytes / (1024 * 1024),
        total_bytes as f64 / (time.as_secs_f64() * 1024.0 * 1024.0),
        total_first.checked_div(samples),
        total_last.checked_div(samples),
    );
}

#[derive(Debug, StructOpt)]
struct Location {
    #[structopt(long = "bucket", default_value = "abk-test-rusoto-download")]
    bucket: String,
    #[structopt(long = "key-prefix", default_value = "test-object")]
    key_prefix: String,
    #[structopt(long = "region", default_value = "us-east-1")]
    region: Region,
}

#[derive(Debug, StructOpt)]
enum Command {
    Create {
        #[structopt(long = "file-size", help = "File size in MB")]
        file_size: usize,
        #[structopt(long = "num-files")]
        num_files: u64,
        #[structopt(flatten)]
        location: Location,
    },
    Download {
        #[structopt(long = "start-num-workers")]
        start_num_workers: u64,
        #[structopt(long = "samples")]
        samples: u32,
        #[structopt(flatten)]
        location: Location,
    },
}

#[tokio::main]
async fn main() {
    env_logger::try_init().unwrap();
    match Command::from_args() {
        Command::Create {
            file_size,
            num_files,
            location:
                Location {
                    region,
                    bucket,
                    key_prefix,
                },
        } => {
            create_files(
                file_size * 1024 * 1024,
                num_files,
                region,
                &bucket,
                &key_prefix,
            )
            .await
        }
        Command::Download {
            start_num_workers,
            samples,
            location:
                Location {
                    region,
                    bucket,
                    key_prefix,
                },
        } => {
            println!("Time, Bytes downloaded, Throughput, Avg first byte, Avg last byte");
            for num_workers in (start_num_workers..80).step_by(5) {
                download_test(num_workers, samples, region.clone(), &bucket, &key_prefix).await;
            }
        }
    }
}
