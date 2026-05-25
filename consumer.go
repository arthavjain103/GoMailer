package main

import "fmt"
import "sync"
import "time"
import "net/smtp"
import "log"
import "os"
import "github.com/joho/godotenv"

func init() {
    godotenv.Load()
}

func emailWorker(id int , ch chan Recipient , wg *sync.WaitGroup) {
	defer wg.Done()
	for recipient := range ch {
		
		
		smtpHost := os.Getenv("smtpHost")
		smtpPort := os.Getenv("smtpPort")

		from := os.Getenv("from")
		smtpUser := os.Getenv("SMTP_USER")
		smtpPass := os.Getenv("SMTP_PASSWORD")
		
		if smtpUser == "" || smtpPass == "" {
			log.Printf("Worker %d: Missing SMTP credentials (set SMTP_USER and SMTP_PASSWORD env vars)\n", id)
			continue
		}
		
		msg , err:= Template(recipient)
		if err != nil {
			log.Printf("Worker %d: Template error for %s: %v\n" , id , recipient.Email, err)
			continue
		}
		
		log.Printf("Worker %d: SMTP Host: %s, SMTP User: %s\n", id, smtpHost, smtpUser)
		
		// SMTP Authentication
		auth := smtp.PlainAuth(
			"",
			smtpUser,
			smtpPass,
			smtpHost,
		)
		fmt.Printf("Worker %d : sending email to %s (from: %s)\n" , id , recipient.Email, from)
		err = smtp.SendMail(smtpHost + ":"+ smtpPort , auth ,  "arthav470@gmail.com" , []string{recipient.Email} , []byte (msg))

		if err != nil{
			log.Printf("Worker %d: SMTP ERROR for %s: %v\n", id, recipient.Email, err)
			continue
		}
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("Worker %d : sent email to %s \n" , id , recipient.Email)
		
	}
}