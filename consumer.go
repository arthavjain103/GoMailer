package main

import (
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

func emailWorker(id int, ch chan Recipient, wg *sync.WaitGroup, client *redislib.Client) {
	defer wg.Done()

	for recipient := range ch {

		err := sendEmail("Main", id, recipient)

		if err != nil {
			log.Printf("Worker %d failed for %s: %v\n",
				id,
				recipient.Email,
				err,
			)


			pushToRetryQueue(client, recipient)
			 removeFromProcessingQueue(client, recipient)
			continue
		}

	    removeFromProcessingQueue(client, recipient)

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

