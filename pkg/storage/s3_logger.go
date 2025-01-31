package storage

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		Bucket: &l.bucket,
		Key:    &filename,
		Body:   bytes.NewReader([]byte(content)),
		ContentType: &([]string{"text/plain"}[0]),
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
