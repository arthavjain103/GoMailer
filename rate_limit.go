package main

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	capacity   float64
	tokens     float64
	rate       float64 // tokens per second
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(capacity, rate float64) *TokenBucket {
	if rate <= 0 {
        panic("rate must be > 0")
    }
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		rate:       rate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = min(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.lastRefill = now
}

// TryAcquire is non-blocking — returns false immediately if no token available
func (tb *TokenBucket) TryAcquire() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// Acquire blocks until a token is available or context is cancelled
func (tb *TokenBucket) Acquire(ctx context.Context) error {
	for {
		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}
		// Wait only for the token deficit, not a full interval
		deficit := 1.0 - tb.tokens
		waitTime := time.Duration(deficit / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err() // caller cancelled or timed out
		case <-time.After(waitTime):
			// retry after wait
		}
	}
}

func (tb *TokenBucket) AvailableTokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}

// min is a helper for Go versions before 1.21
// Remove this if your project uses Go 1.21+
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}