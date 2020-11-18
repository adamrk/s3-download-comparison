package main

import (
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	sess, _ := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	client := s3.New(sess, aws.NewConfig())
	threads := 2
	samples := 3
	fmt.Println("Total time, Total bytes, Throughput, Avg first byte, Avg last byte")
	runTests(client, threads, samples)
}

type latency struct {
	firstByte time.Duration
	lastByte  time.Duration
}

func runTests(s3Client *s3.S3, threads int, samples int) {
	objectSize := 8388608
	threadCount := threads
	// a channel to submit the test tasks
	testTasks := make(chan int, threadCount)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan latency, samples)

	bucketName := "abk-test-rusoto-download"

	// create the workers for all the threads in this test
	for w := 1; w <= threadCount; w++ {
		go func(o int, tasks <-chan int, results chan<- latency) {
			for range tasks {
				// generate an S3 key from the sha hash of the hostname, thread index, and object size
				key := fmt.Sprintf("test-object-%d-%d", objectSize, o%40)

				// start the timer to measure the first byte and last byte latencies
				latencyTimer := time.Now()

				// do the GetObject request
				req := s3.GetObjectInput(s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				})

				resp, err := s3Client.GetObject(&req)

				// if a request fails, exit
				if err != nil {
					panic("Failed to get object: " + err.Error())
				}

				// measure the first byte latency
				firstByte := time.Now().Sub(latencyTimer)

				// create a buffer to copy the S3 object body to
				var buf = make([]byte, objectSize)

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

				// measure the last byte latency
				lastByte := time.Now().Sub(latencyTimer)

				// add the latency result to the results channel
				results <- latency{firstByte, lastByte}
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
	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		latency := <-results
		sumFirstByte += latency.firstByte.Nanoseconds()
		sumLastByte += latency.lastByte.Nanoseconds()
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)

	// calculate the throughput rate
	rate := (float64(objectSize)) / (totalTime.Seconds()) / 1024 / 1024

	// print the results to stdout
	fmt.Printf(
		"%9.4f s, %v B, %9.1f MB/s, %5.0f ms, %5.0f ms\n",
		totalTime.Seconds(),
		objectSize*samples,
		rate,
		float64(sumFirstByte)/float64(samples)/1000000,
		float64(sumLastByte)/float64(samples)/1000000)
}
