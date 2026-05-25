package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisAddr   string
	PostgresDSN string
	WorkerCount int
}

func Load() *Config {
	godotenv.Load()
	workerCount, err := strconv.Atoi(os.Getenv("WORKER_COUNT"))
	if err != nil {
		workerCount = 1
	}

	return &Config{
		RedisAddr:   os.Getenv("REDIS_ADDR"),
		PostgresDSN: os.Getenv("POSTGRES_DSN"),
		WorkerCount: workerCount,
	}
}
