package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "sync"

    "github.com/joho/godotenv"
    redislib "github.com/redis/go-redis/v9"
)

const (
	MAX_RETRIES = 3
)

func init() {
	godotenv.Load()
}

func emailWorker(id int, ch chan Recipient, wg *sync.WaitGroup, client *redislib.Client ,   bucket *TokenBucket,) {
	defer wg.Done()

	for recipient := range ch {

		// Dashboard "Stop/Pause" control: block picking up new work while paused.
		// The item stays in email:processing (already recorded there via BLMove),
		// so nothing is dropped or reordered - it simply waits here until resumed.
		waitWhilePaused()

        err := bucket.Acquire(context.Background())
		if err != nil {
        continue
    }

		err = sendEmail("Main", id, recipient)

		if err != nil {
			log.Printf("Worker %d failed for %s: %v\n",
				id,
				recipient.Email,
				err,
			)

			willRetry := recipient.Retry+1 <= MAX_RETRIES
			pushToRetryQueue(client, recipient)
			 removeFromProcessingQueue(client, recipient)
			if willRetry {
				appendActivityLog("retry", recipient.Email, fmt.Sprintf("worker %d: send failed, queued for retry (%d/%d)", id, recipient.Retry+1, MAX_RETRIES))
			} else {
				client.Incr(ctx, "stats:failed")
				appendActivityLog("failed", recipient.Email, fmt.Sprintf("worker %d: send failed, moved to DLQ after %d attempts", id, recipient.Retry+1))
			}
			continue
		}

	    removeFromProcessingQueue(client, recipient)
		client.Incr(ctx, "stats:completed")
		appendActivityLog("sent", recipient.Email, fmt.Sprintf("worker %d: email sent successfully", id))

		fmt.Printf("Worker %d: email sent successfully to %s\n",
			id,
			recipient.Email,
		)
	}
}
// pushToRetryQueue handles retry logic: increment retry count, push to retry queue or DLQ
func pushToRetryQueue(client *redislib.Client, recipient Recipient) {
	recipient.Retry++
	

	// If max retries exceeded, push to Dead Letter Queue
	if recipient.Retry > MAX_RETRIES {
		jsonData, _ := json.Marshal(recipient)
		err := client.RPush(ctx, "email:dlq", string(jsonData)).Err()
		if err != nil {
			log.Printf("Error pushing to DLQ: %v\n", err)
		}
		log.Printf("Email %s moved to DLQ after %d retries\n", recipient.Email, recipient.Retry-1)
		return
	}

	// Push to retry queue
	jsonData, _ := json.Marshal(recipient)
	err := client.RPush(ctx, "email:retry", string(jsonData)).Err()
	if err != nil {
		log.Printf("Error pushing to retry queue: %v\n", err)
		return
	}

	log.Printf("Email %s queued for retry (Attempt %d/%d)\n", recipient.Email, recipient.Retry, MAX_RETRIES)
}

