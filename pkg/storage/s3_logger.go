package storage

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

// UploadLog uploads a log to the specified S3 bucket
func (l *S3Logger) UploadLog(filename string, content string) {
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      &l.bucket,
		Key:         &filename,
		Body:        bytes.NewReader([]byte(content)),
		ContentType: awsString("text/plain"),
		Metadata: map[string]string{
			"Timestamp": time.Now().Format(time.RFC3339),
		},
	})
	if err != nil {
		log.Printf("Failed to upload log to S3: %v", err)
		return
	}
	log.Printf("Successfully uploaded log to S3: %s", filename)
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
