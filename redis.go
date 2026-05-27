package main

import (
	"context"
	"fmt"
	"log"
 "os"
	redislib "github.com/redis/go-redis/v9"
)

var ctx = context.Background()
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

	return client
}