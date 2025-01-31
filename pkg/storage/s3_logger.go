package storage

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Max retries for S3 uploads
const maxRetries = 3

// S3Logger uploads logs to S3
type S3Logger struct {
	client *s3.Client
	bucket string
}

// NewS3Logger initializes the logger with AWS SDK V2
func NewS3Logger(bucket, region string) *S3Logger {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Unable to load AWS SDK config: %v", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Logger{
		client: client,
		bucket: bucket,
	}
}

// UploadLog retries S3 log upload with exponential backoff and falls back to local storage if all retries fail.
func (l *S3Logger) UploadLog(filename string, content string) {
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Attempt %d to upload log to S3: %s", attempt, filename)

		err = l.uploadToS3(filename, content)
		if err == nil {
			log.Printf("Successfully uploaded log to S3: %s", filename)
			return
		}

		// Exponential backoff
		wait := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		log.Printf("Upload failed: %v. Retrying in %s...", err, wait)
		time.Sleep(wait)
	}

	// All retries failed, fallback to local storage
	log.Printf("Failed to upload log to S3 after %d attempts. Saving locally.", maxRetries)
	l.saveLogLocally(filename, content)
}

// uploadToS3 performs the actual upload to S3.
func (l *S3Logger) uploadToS3(filename string, content string) error {
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &l.bucket,
		Key:    &filename,
		Body:   bytes.NewReader([]byte(content)),
	})
	return err
}

// saveLogLocally saves logs locally when S3 uploads fail.
func (l *S3Logger) saveLogLocally(filename string, content string) {
	localFilePath := fmt.Sprintf("local_logs/%s", filename)
	if err := os.MkdirAll("local_logs", os.ModePerm); err != nil {
		log.Printf("Failed to create local log directory: %v", err)
		return
	}

	file, err := os.Create(localFilePath)
	if err != nil {
		log.Printf("Failed to create local log file: %v", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(content); err != nil {
		log.Printf("Failed to write log content locally: %v", err)
	} else {
		log.Printf("Log saved locally: %s", localFilePath)
	}
}

// Helper function to convert string to *string (since AWS SDK requires pointers)
func awsString(value string) *string {
	return &value
}

// ListLogFiles lists all log files for a specific task in S3.
func (l *S3Logger) ListLogFiles(prefix string) ([]string, error) {
	output, err := l.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &l.bucket,
		Prefix: &prefix,
	})
	if err != nil {
		log.Printf("Failed to list objects in S3 with prefix %s: %v", prefix, err)
		return nil, err
	}

	var logFiles []string
	for _, obj := range output.Contents {
		logFiles = append(logFiles, *obj.Key)
	}

	return logFiles, nil
}

// FetchLogContent fetches the content of a log file from S3.
func (l *S3Logger) FetchLogContent(key string) (string, error) {
	output, err := l.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &l.bucket,
		Key:    &key,
	})
	if err != nil {
		log.Printf("Failed to fetch log content for key %s: %v", key, err)
		return "", err
	}
	defer output.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(output.Body)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
