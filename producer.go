package main


import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

func loadRecipient(filePath string , client *redislib.Client ) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		return err
	}

		// Loop through CSV rows
	for _, record := range records[1:] {

		recipient := Recipient{
			Name:  record[0],
			Email: record[1],
		}
		//IDEMPOTENCY CHECK
		key := "email:" + recipient.Email 
		set, err := client.SetNX(ctx, key, 1, 24*time.Hour).Result()
		if err != nil {
			fmt.Println("Redis error:", err)
			continue
		}

		if !set {
			fmt.Println("Duplicate email skipped:", recipient.Email)
			continue
		}

		// Convert struct -> JSON
		jsonData, err := json.Marshal(recipient)
		if err != nil {
			fmt.Println("JSON error:", err)
			continue
		}
		

		// Push into Redis queue
		err = client.RPush(ctx, "email:queue", jsonData).Err()
		if err != nil {
			fmt.Println("Redis push error:", err)
			continue
		}

		fmt.Println("Added to Redis Queue:", recipient.Email)
	}

	return nil
}


