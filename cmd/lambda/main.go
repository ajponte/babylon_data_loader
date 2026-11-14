package main

import (
	"context"
	"log/slog"
	"os"

	bcontext "babylon/dataloader/appcontext"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := bcontext.WithLogger(context.Background(), logger)
	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to load AWS SDK config", "error", err)
		os.Exit(1)
	}

	s3Client := s3.NewFromConfig(sdkConfig)
	smClient := secretsmanager.NewFromConfig(sdkConfig)
	secretID := os.Getenv("MONGO_SECRET_ID")
	if secretID == "" && os.Getenv("MONGO_URI") == "" {
		logger.ErrorContext(ctx, "Cold start validation failed: MONGO_SECRET_ID or MONGO_URI must be set")
		os.Exit(1)
	}

	logger.InfoContext(ctx, "Lambda cold start initialization complete", "secretID", secretID)

	handler := NewS3EventHandler(s3Client, smClient, secretID, nil, logger)
	lambda.Start(handler.HandleS3Event)
}
