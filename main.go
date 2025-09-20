// main.go
package main

import (
	"log/slog"
	"os"
)

func main() {
	// Create the logger instance at the very beginning.
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Fix for noinlineerr: Separate the assignment and the error check.
	run(logger)
}

func run(logger *slog.Logger) {
	logger.Info("Entering run scope.")
}
