package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"text/template"

)



type Recipient struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Retry int   `json:"retry"`
}

func main() {

	recipientChannel := make(chan Recipient, 50)
	
	var wg sync.WaitGroup

	// Redis init
	client := InitRedis()

	// PRODUCER (CSV → Redis)
	
	go func() {
		err := loadRecipient("dummy_emails.csv", client)
		if err != nil {
			fmt.Println("Producer Error:", err)
		}
	}()


	// CONSUMER (Redis → Channel)

	go func() {
		for {
			data, err := client.BLMove(ctx, "email:queue", "email:processing", "LEFT", "RIGHT", 0).Result()
			if err != nil {
				fmt.Println("Redis Error:", err)
				continue
			}

	
		var recipient Recipient
		err = json.Unmarshal([]byte(data), &recipient)
		if err != nil {
			fmt.Println("JSON Error:", err)
			continue
		}

		recipientChannel <- recipient
		}
	}()


	// RETRY CONSUMER (Retry Queue → Retry Channel)
	
	go func() {
		for {
		data, err := client.BLMove(
			ctx,
			"email:retry",      
			"email:processing",
			"LEFT",
			"RIGHT",
			0,
		).Result()
if err != nil {
	fmt.Println("Retry Redis Error:", err)
	continue
}
	
		var recipient Recipient
		err = json.Unmarshal([]byte(data), &recipient)
		if err != nil {
			fmt.Println("Retry JSON Error:", err)
				continue
			}

			fmt.Printf("Processing retry for %s (Attempt %d/%d)\n", recipient.Email, recipient.Retry, MAX_RETRIES)
			recipientChannel <- recipient
		}
	}()

	
	// WORKERS (for main queue)
	
	workerCount := 5

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go emailWorker(i, recipientChannel, &wg, client)
	}



	wg.Wait()
	fmt.Println("All email workers completed!")
}

func Template(r Recipient) (string, error) {
	t, err := template.ParseFiles("email.tmpl")
	if err != nil {
		return "", err
	}
	var tpl bytes.Buffer
	err = t.Execute(&tpl, r)
	if err != nil {
		return "", err
	}
	return tpl.String(), nil
}