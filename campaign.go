package main

import (
	"crypto/rand"
	"fmt"
	"time"

	redislib "github.com/redis/go-redis/v9"
)



const (
	currentCampaignIDKey     = "campaign:current:id"
	currentCampaignSourceKey = "campaign:current:source"
)

// newCampaignID returns a random UUID v4 string. Implemented locally with
// crypto/rand instead of pulling in a UUID library, to keep go.mod
// unchanged.
func newCampaignID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Extremely unlikely — fall back to a timestamp-based id so the
		// campaign flow never breaks even if crypto/rand fails.
		return fmt.Sprintf("campaign-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// setCurrentCampaign persists which campaign is "active" (tied to the most
// recently uploaded CSV) in Redis.
func setCurrentCampaign(client *redislib.Client, source, campaignID string) error {
	if err := client.Set(ctx, currentCampaignIDKey, campaignID, 0).Err(); err != nil {
		return err
	}
	return client.Set(ctx, currentCampaignSourceKey, source, 0).Err()
}

// getCurrentCampaign reads back the active campaign (source file + ID) from
// Redis. ok is false if none has ever been set (e.g. a brand new Redis
// instance that has never had a CSV uploaded or bootstrapped).
func getCurrentCampaign(client *redislib.Client) (source, campaignID string, ok bool) {
	id, err := client.Get(ctx, currentCampaignIDKey).Result()
	if err != nil || id == "" {
		return "", "", false
	}
	src, err := client.Get(ctx, currentCampaignSourceKey).Result()
	if err != nil {
		src = ""
	}
	return src, id, true
}
