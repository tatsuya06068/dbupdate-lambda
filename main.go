package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type MyEvent struct {
	DbURL        string `json:"db_url"`
	S3Bucket     string `json:"s3_bucket"`
	S3KeyPrefix  string `json:"s3_key_prefix"`
}

func handleRequest(event MyEvent) (string, error) {
	dbURL := event.DbURL
	s3Bucket := event.S3Bucket
	s3KeyPrefix := event.S3KeyPrefix

	if dbURL == "" || s3Bucket == "" || s3KeyPrefix == "" {
		return "", fmt.Errorf("db_url, s3_bucket, and s3_key_prefix are required")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // 必要に応じてリージョンを変更
	})
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %w", err)
	}

	s3Client := s3.New(sess)

	tempDir, err := ioutil.TempDir("", "migrations")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// S3 からマイグレーションファイルをダウンロード
	if err := downloadMigrations(s3Client, s3Bucket, s3KeyPrefix, tempDir); err != nil {
		return "", fmt.Errorf("failed to download migrations: %w", err)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", tempDir),
		dbURL,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create migrate instance: %w", err)
	}

	defer func() {
		if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
			log.Printf("error closing migrate instance: srcErr=%v, dbErr=%v", srcErr, dbErr)
		}
	}()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return "", fmt.Errorf("failed to apply migrations: %w", err)
	}

	return "Migration completed successfully", nil
}

func downloadMigrations(s3Client *s3.S3, bucket, prefix, destDir string) error {
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	err := s3Client.ListObjectsV2Pages(listInput, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			key := *item.Key
			filePath := filepath.Join(destDir, filepath.Base(key))

			getInput := &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}

			result, err := s3Client.GetObject(getInput)
			if err != nil {
				log.Printf("failed to get object %s: %v", key, err)
				return false
			}

			defer result.Body.Close()

			buf := new(bytes.Buffer)
			buf.ReadFrom(result.Body)
			if err := ioutil.WriteFile(filePath, buf.Bytes(), 0644); err != nil {
				log.Printf("failed to write file %s: %v", filePath, err)
				return false
			}
		}
		return true
	})

	return err
}

func main() {
	lambda.Start(handleRequest)
}