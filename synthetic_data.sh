#!/bin/bash

# Build the Go application
go build -o app

# Run the application to generate synthetic data
./app generate-synthetic-data --dir tmp/synthetic
