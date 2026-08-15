package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var DB *sql.DB

type EmailEvent struct {
	ID         int64
	CampaignID string
	Email      string
	Status     string
	RetryCount int
	CreatedAt  time.Time
}

func getPostgresDSN() string {
	if dsn := strings.TrimSpace(os.Getenv("POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5432/gomailer?sslmode=disable"
}

func InitPostgres() *sql.DB {
	dsn := getPostgresDSN()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Printf("Postgres connection error: %v\n", err)
		return nil
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		log.Printf("Postgres ping failed: %v\n", err)
		_ = db.Close()
		return nil
	}

	if err := ensurePostgresSchema(db); err != nil {
		log.Printf("Postgres schema setup failed: %v\n", err)
		_ = db.Close()
		return nil
	}

	DB = db
	fmt.Println("Connected to PostgreSQL")
	return db
}

func ensurePostgresSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS campaign_email_events (
			id SERIAL PRIMARY KEY,
			campaign_id TEXT NOT NULL,
			email TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_campaign_email_events_campaign_id
		ON campaign_email_events (campaign_id);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_campaign_email_events_status
		ON campaign_email_events (status);
	`)
	return err
}

func RecordEmailEvent(campaignID, email, status string, retryCount int) error {
	if DB == nil {
		return nil
	}

	_, err := DB.Exec(
		`INSERT INTO campaign_email_events (campaign_id, email, status, retry_count) VALUES ($1, $2, $3, $4)`,
		campaignID,
		email,
		status,
		retryCount,
	)
	return err
}

func GetCampaignAnalytics(campaignID string) (map[string]int, error) {
	if DB == nil {
		return map[string]int{}, nil
	}

	query := `SELECT status, COUNT(*) FROM campaign_email_events`
	args := []interface{}{}
	if strings.TrimSpace(campaignID) != "" {
		query += " WHERE campaign_id = $1"
		args = append(args, campaignID)
	}
	query += " GROUP BY status"

	rows, err := DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summary := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		summary[status] = count
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return summary, nil
}

func SummarizeEmailEvents(events []EmailEvent) map[string]int {
	summary := make(map[string]int)
	for _, event := range events {
		if event.Status == "" {
			continue
		}
		summary[event.Status]++
	}
	return summary
}
