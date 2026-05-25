# Go Email Sender Project

A high-performance concurrent bulk email sender built entirely in Go using goroutines, channels, and worker pools. The application reads recipient data from CSV files, generates personalized emails using Go templates, and sends them efficiently through SMTP integration.

The project is designed to understand real-world concurrency patterns in Go while building a scalable background email delivery system capable of processing thousands of recipients with controlled resource usage.

---

# Overview

This application demonstrates professional-grade concurrent email delivery with the following capabilities:

- Process bulk email campaigns from CSV files with minimal memory footprint
- Send personalized emails to thousands of recipients using Go's template system
- Maintain controlled concurrency through worker pool pattern to prevent resource exhaustion
- Integrate seamlessly with SMTP providers like Brevo for reliable email delivery
- Handle errors gracefully with per-worker logging and recovery mechanisms

---

# Features

- **Concurrent Bulk Email Sending:** Utilizes worker pool pattern with multiple goroutines processing emails in parallel
- **Buffered Channel Queue:** Decouples CSV producer from email workers to optimize throughput
- **CSV-based Recipient Management:** Load recipient data directly from CSV files with automatic parsing
- **Dynamic Email Templating:** Generate personalized emails using Go's `text/template` package with recipient-specific data
- **SMTP Integration:** Send emails through standard SMTP protocol with authentication
- **Environment-based Configuration:** Store SMTP credentials and settings in environment variables for security
- **Graceful Synchronization:** Use `sync.WaitGroup` to ensure all workers complete before application exit
- **Personalized Email Generation:** Replace template variables like Name and Email for each recipient
- **Worker-level Logging:** Each worker logs its activity with success and error details

---

# Architecture

## Data Flow

The application follows a clean pipeline architecture:

1. CSV Loader (Producer) reads recipient data from file
2. Recipient objects are placed into buffered channel
3. Worker goroutines concurrently consume from channel
4. Template engine personalizes email content for each recipient
5. SMTP client sends completed email to recipient
6. WaitGroup synchronizes graceful application shutdown

---

# Key Architectural Decisions

## Buffered Channels

The application uses buffered channels with capacity 50 to decouple the producer (CSV loader) from consumers (email workers):

```go
recipientChannel := make(chan Recipient, 50)
```

### Why this matters

Instead of the CSV loader blocking when workers are busy, recipients can be queued in the buffer. This allows the loader to continue processing while workers handle the queue, resulting in better throughput and reduced idle time. The 50-capacity buffer balances memory usage against queue responsiveness.

---

## Worker Pool Pattern

A fixed pool of 5 workers processes emails concurrently instead of creating unlimited goroutines:

```go
workerCount := 5

for i := 1; i <= workerCount; i++ {
    wg.Add(1)
    go emailWorker(i, recipientChannel, &wg)
}
```

### Why this matters

Creating a new goroutine per email would exhaust system resources with large datasets. A fixed pool provides controlled concurrency, prevents SMTP connection overload, and keeps memory usage predictable. The pool size can be tuned based on SMTP rate limits and infrastructure capacity.

---

## Environment Variables for Security

SMTP credentials are stored in environment variables rather than hardcoded in source:

```go
smtpUser := os.Getenv("SMTP_USER")
smtpPass := os.Getenv("SMTP_PASSWORD")
```

### Why this matters

Sensitive credentials remain out of version control and source code. Environment-based configuration also enables different settings across development, staging, and production environments without code changes.

---

## Template-Based Email Rendering

Go's `text/template` package generates personalized emails dynamically:

```go
template content: "Hello {{.Name}}, your email is {{.Email}}"
```

### Why this matters

Separating template content from application logic makes emails reusable, easy to customize, and maintainable. Non-technical team members can modify email content without touching code.

---

## Graceful Synchronization

`sync.WaitGroup` ensures all workers complete before shutdown:

```go
wg.Wait()
```

### Why this matters

The application won't exit until all queued recipients have been processed. This prevents data loss from partial execution and guarantees complete email delivery for the batch.

---

# Current Processing Flow

```text
CSV File (recipients with name and email)
-> Parsed by loadRecipient function
-> Recipients added to buffered channel (capacity: 50)
-> 5 worker goroutines consume recipients concurrently
-> Each worker generates personalized email from template
-> Email sent via SMTP to recipient
-> Worker logs success or error
-> On channel close, workers finish processing
-> sync.WaitGroup waits for all workers to complete
-> Application exits
```

---

# Setup Instructions

## Prerequisites

- Go 1.18 or higher installed on your system
- SMTP credentials (from Brevo or similar SMTP provider)
- CSV file with recipient data in format: Name, Email

---

## Configuration

Create a `.env` file in the project root:

```env
smtpHost=smtp-relay.brevo.com
smtpPort=587
SMTP_USER=your_brevo_email@example.com
SMTP_PASSWORD=your_brevo_smtp_password
from=noreply@yourdomain.com
```

Prepare a CSV file with recipients in format:

```csv
Name,Email
John Smith,john@example.com
Jane Doe,jane@example.com
```

---

## Running the Application

```bash
go run main.go producer.go consumer.go
```

The application will read the CSV file, send emails using 5 concurrent workers, and exit when all recipients have been processed.

---

# Future Enhancements

## Current Focus Areas

### Frontend Dashboard

Web-based interface for uploading CSV files, monitoring delivery status, and viewing campaign analytics. Users can upload new recipient lists directly through the browser without command-line interaction.

### Rate Limiter

Implement request throttling to respect SMTP provider limits, preventing account blocks or rate limiting penalties. Features include configurable emails per minute and automatic backoff on failure responses from SMTP server.

### Multiple Template Support

Extend system to support different email templates based on recipient type or campaign category. CSV column will specify which template to use for each recipient, enabling multi-variant campaigns from single batch.

---

## Planned Features Beyond Current Scope

- Retry mechanism for failed email delivery with exponential backoff
- Duplicate detection to prevent resending to same recipient
- Database integration for persistent delivery records
- Queue persistence using Redis or RabbitMQ for crash recovery

---

# Tech Stack

- **Language:** Go (Golang)
- **Concurrency:** Goroutines and Channels
- **Email Protocol:** Brevo SMTP Relay
- **Template Engine:** Go `text/template` package
- **Data Format:** CSV processing
- **Configuration:** Environment variables
- **SMTP Provider:** Brevo (compatible with standard SMTP servers)

---

# Project Structure

```text
main.go            - Application entry point, goroutine coordination, template rendering
producer.go        - CSV file reading and recipient loading into channel
consumer.go        - Email worker implementation, SMTP sending, error handling
email.tmpl         - Email template with recipient personalization
dummy_emails.csv   - Sample recipient data for testing
.env               - Environment configuration (credentials, SMTP settings)
```

---

# Author

Arthav Jain