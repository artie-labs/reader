package main

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

func main() {
	os.Setenv("TZ", "UTC")
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelInfo})))

}
