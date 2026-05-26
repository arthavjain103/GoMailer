package main

import (
	"fmt"
	"net/smtp"
	"os"
	"encoding/json"
	"log"

	redislib "github.com/redis/go-redis/v9"
)

// Common reusable email sender
func sendEmail(workerType string, id int, recipient Recipient) error {

	smtpHost := os.Getenv("smtpHost")
	smtpPort := os.Getenv("smtpPort")
	from := os.Getenv("SMTP_FROM")
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASSWORD")

	// Generate email template
	msg, err := Template(recipient)
	if err != nil {
		return fmt.Errorf("template error: %v", err)
	}

	// SMTP Authentication
	auth := smtp.PlainAuth(
		"",
		smtpUser,
		smtpPass,
		smtpHost,
	)

	fmt.Printf(
		"%s Worker %d: sending email to %s (Retry: %d)\n",
		workerType,
		id,
		recipient.Email,
		recipient.Retry,
	)

	// Send Email
	err = smtp.SendMail(
		smtpHost+":"+smtpPort,
		auth,
		from,
		[]string{recipient.Email},
		[]byte(msg),
	)

	if err != nil {
		return fmt.Errorf("smtp error: %v", err)
	}

	return nil
}


// removeFromProcessingQueue removes successfully processed jobs
func removeFromProcessingQueue(
	client *redislib.Client,
	recipient Recipient,
) {

	jsonData, err := json.Marshal(recipient)
	if err != nil {
		log.Printf(
			"Marshal error while removing processing job: %v\n",
			err,
		)
		return
	}

	err = client.LRem(
		ctx,
		"email:processing",
		1,
		string(jsonData),
	).Err()

	if err != nil {
		log.Printf(
			"Redis cleanup error: %v\n",
			err,
		)
	}
}