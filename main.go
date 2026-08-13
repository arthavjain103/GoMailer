package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/template"
)

func shouldAutoLoadDummyCSV() bool {
	v := strings.TrimSpace(os.Getenv("AUTO_LOAD_DUMMY_CSV"))
	return strings.EqualFold(v, "true") || v == "1" || strings.EqualFold(v, "yes")
}

type Recipient struct {
	Name       string `json:"name"`
	Email      string `json:"email"`
	Retry      int    `json:"retry"`
	CampaignID string `json:"campaign_id"`
}

func main() {

	recipientChannel := make(chan Recipient, 50)

	var wg sync.WaitGroup

	// Redis init
	client := InitRedis()
	recoverProcessing(client)

	// HTTP API for the React dashboard (status/stats/upload/campaign controls).
	// Runs independently of the CSV → Redis → workers pipeline below.
	go StartServer()
	bucket := NewTokenBucket(
		20, // burst capacity
		2,  // 2 emails/sec
	)

	// Load the default template text from disk so behaviour is unchanged
	// unless the user edits/saves a new template from the dashboard's
	// "Template" page (POST /api/template).
	defaultTemplate, err := os.ReadFile("email.tmpl")
	if err != nil {
		panic(err)
	}
	setEmailTemplateText(string(defaultTemplate))

	// PRODUCER (CSV → Redis)
	//
	// On a brand-new app start there is no campaign state in Redis yet. By
	// default we intentionally do not auto-load the bundled sample CSV; the app
	// should stay idle until the user uploads a new file or explicitly starts a
	// campaign. If someone wants the legacy startup behavior, they can opt in by
	// setting AUTO_LOAD_DUMMY_CSV=true.
	if source, campaignID, ok := getCurrentCampaign(client); ok {
		setLastUploadedCSV(source)
		appendActivityLog("info", "", fmt.Sprintf("resumed existing campaign %s after restart", campaignID))
	} else if shouldAutoLoadDummyCSV() {
		bootstrapID := newCampaignID()
		bootstrapSource := "dummy_emails.csv"
		if err := setCurrentCampaign(client, bootstrapSource, bootstrapID); err != nil {
			fmt.Println("Campaign bootstrap error:", err)
		}
		setLastUploadedCSV(bootstrapSource)

		go func() {
			err := loadRecipient(bootstrapSource, client, bootstrapID)
			if err != nil {
				fmt.Println("Producer Error:", err)
			}
		}()
	} else {
		appendActivityLog("info", "", "no active campaign found; waiting for upload or manual start")
	}

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
		go emailWorker(i, recipientChannel, &wg, client, bucket)
	}

	wg.Wait()
	fmt.Println("All email workers completed!")

}

func Template(r Recipient) (string, error) {

	// Parse the current template text (editable from the dashboard's
	// Template page; falls back to whatever was loaded from email.tmpl at
	// startup if never edited). Parsing per-send keeps this change small
	// and self-contained instead of adding extra caching/invalidation.
	tmpl, err := template.New("email").Parse(getEmailTemplateText())
	if err != nil {
		return "", err
	}

	var tpl bytes.Buffer

	err = tmpl.Execute(&tpl, r)
	if err != nil {
		return "", err
	}

	return tpl.String(), nil
}
