package main

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	region    string
	bucket    string
	keyPrefix string
	workers   int
	samples   int
)

func parseFlags() {
	regionArg := flag.String("region", "us-east-1", "AWS region")
	bucketArg := flag.String("bucket", "abk-test-rusoto-download", "Bucket to download from")
	keyPrefixArg := flag.String("prefix", "test-object-8388608", "Common prefix for test objects")
	workersArg := flag.Int("workers", 3, "Number of workers to run")
	samplesArg := flag.Int("samples", 10, "Number of downloads to do")

	flag.Parse()

	region = *regionArg
	bucket = *bucketArg
	keyPrefix = *keyPrefixArg
	workers = *workersArg
	samples = *samplesArg
}

func main() {
	parseFlags()

	sess, _ := session.NewSession(&aws.Config{Region: aws.String(region)})
	client := s3.New(sess, aws.NewConfig())
	fmt.Println("Total time, Total bytes, Throughput, Avg first byte, Avg last byte")
	runTests(client, workers, samples)
}

type stats struct {
	bytes     int64
	firstByte time.Duration
	lastByte  time.Duration
}

func runTests(s3Client *s3.S3, workers int, samples int) {
	testTasks := make(chan int, workers)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan stats, samples)

	// create the workers for all the threads in this test
	for w := 1; w <= workers; w++ {
		go func(o int, tasks <-chan int, results chan<- stats) {
			for range tasks {
				// generate an S3 key from the sha hash of the hostname, thread index, and object size
				key := fmt.Sprintf("%v-%d", keyPrefix, o%40)

				// start the timer to measure the first byte and last byte latencies
				latencyTimer := time.Now()

				// do the GetObject request
				req := s3.GetObjectInput(s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})

				resp, err := s3Client.GetObject(&req)

				// if a request fails, exit
				if err != nil {
					panic("Failed to get object: " + err.Error())
				}

				// measure the first byte stats
				firstByte := time.Now().Sub(latencyTimer)
				bytes := *resp.ContentLength

				// create a buffer to copy the S3 object body to
				var buf = make([]byte, bytes)

				// read the s3 object body into the buffer
				size := 0
				for {
					n, err := resp.Body.Read(buf)

					size += n

					if err == io.EOF {
						break
					}

					// if the streaming fails, exit
					if err != nil {
						panic("Error reading object body: " + err.Error())
					}
				}

				_ = resp.Body.Close()

				// measure the last byte stats
				lastByte := time.Now().Sub(latencyTimer)

				// add the stats result to the results channel
				results <- stats{bytes, firstByte, lastByte}
			}
		}(w, testTasks, results)
	}

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	for j := 1; j <= samples; j++ {
		testTasks <- j
	}

	// close the channel
	close(testTasks)

	sumFirstByte := int64(0)
	sumLastByte := int64(0)
	totalBytes := int64(0)
	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		stats := <-results
		sumFirstByte += stats.firstByte.Nanoseconds()
		sumLastByte += stats.lastByte.Nanoseconds()
		totalBytes += stats.bytes
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)

	// calculate the throughput rate
	rate := (float64(totalBytes)) / (totalTime.Seconds()) / 1024 / 1024

	// print the results to stdout
	fmt.Printf(
		"%9.4f s, %v B, %6.1f MB/s, %5.0f ms, %5.0f ms\n",
		totalTime.Seconds(),
		totalBytes,
		rate,
		float64(sumFirstByte)/float64(samples)/1000000,
		float64(sumLastByte)/float64(samples)/1000000)
}
