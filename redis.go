package main

import (
	"context"
	"fmt"
	"log"
 "os"
	redislib "github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// RedisClient is a package-level handle to the same client returned by InitRedis.
// Added so the new HTTP server (server.go) can read queue lengths / counters
// without altering how the client is created or used by the existing pipeline.
var RedisClient *redislib.Client

func getRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func InitRedis() *redislib.Client {
	// Connect to Redis
	client := redislib.NewClient(&redislib.Options{
		Addr:     getRedisAddr(), // Replace with your Redis server address
		Password: "",                // No password for local development
		DB:       0,                 // Default DB
	})

	// Ping the Redis server to check the connection
	pong, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
	}
	fmt.Println("Connected to Redis:", pong)

	RedisClient = client
	return client
}