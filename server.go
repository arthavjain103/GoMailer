package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
	"go_project/internal/postgres"
)

// It is a thin read/observe/trigger layer on top of the existing pipeline:
//   - stats endpoints just LLEN the existing Redis lists / read counters
//   - upload endpoint just saves a file to disk (does not touch Redis)
//   - start endpoint just calls the existing loadRecipient() function
//   - stop/start (pause) endpoints just flip the in-memory flag consumer.go
//     already checks between jobs

const uploadsDir = "uploads"

func getPort() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return "8080"
}

// withCORS allows the separately-hosted React dev server (and any static
// build) to call this API during local development.
func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// GET /api/status
func handleStatus(w http.ResponseWriter, r *http.Request) {
	redisOK := true
	if _, err := RedisClient.Ping(ctx).Result(); err != nil {
		redisOK = false
	}

	_, campaignID, _ := getCurrentCampaign(RedisClient)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"redis":       redisOK,
		"paused":      isPaused(),
		"campaign":    getLastUploadedCSV(),
		"campaign_id": campaignID,
		"timestamp":   time.Now(),
	})
}

// GET /api/stats
func handleStats(w http.ResponseWriter, r *http.Request) {
	pending, _ := RedisClient.LLen(ctx, "email:queue").Result()
	processing, _ := RedisClient.LLen(ctx, "email:processing").Result()
	retry, _ := RedisClient.LLen(ctx, "email:retry").Result()
	failed, _ := RedisClient.LLen(ctx, "email:dlq").Result()

	completedStr, err := RedisClient.Get(ctx, "stats:completed").Result()
	completed := int64(0)
	if err == nil {
		fmt.Sscanf(completedStr, "%d", &completed)
	}

	analytics := map[string]int{}
	if _, campaignID, _ := getCurrentCampaign(RedisClient); campaignID != "" {
		if summary, err := postgres.GetCampaignAnalytics(campaignID); err == nil {
			analytics = summary
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pending":    pending,
		"processing": processing,
		"retry":      retry,
		"failed":     failed,
		"completed":  completed,
		"analytics":  analytics,
	})
}

// POST /api/upload  (multipart/form-data, field name "file")
func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	if err := r.ParseMultipartForm(20 << 20); err != nil { // 20MB max
		writeError(w, http.StatusBadRequest, "could not parse upload: "+err.Error())
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing 'file' field")
		return
	}
	defer file.Close()

	if err := os.MkdirAll(uploadsDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "could not create uploads dir")
		return
	}

	safeName := fmt.Sprintf("%d-%s", time.Now().Unix(), filepath.Base(header.Filename))
	destPath := filepath.Join(uploadsDir, safeName)

	dest, err := os.Create(destPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not save file")
		return
	}
	defer dest.Close()

	if _, err := io.Copy(dest, file); err != nil {
		writeError(w, http.StatusInternalServerError, "could not write file")
		return
	}

	setLastUploadedCSV(destPath)

	// A new CSV upload always means a brand new campaign — this is what
	// keeps recipients from being skipped by the per-campaign idempotency
	// key just because they were emailed as part of a previous campaign.
	campaignID := newCampaignID()
	if err := setCurrentCampaign(RedisClient, destPath, campaignID); err != nil {
		writeError(w, http.StatusInternalServerError, "could not register new campaign: "+err.Error())
		return
	}

	appendActivityLog("info", "", fmt.Sprintf("uploaded recipients file: %s (new campaign %s)", header.Filename, campaignID))

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":     "upload successful",
		"filename":    header.Filename,
		"path":        destPath,
		"campaign_id": campaignID,
	})
}

// POST /api/campaign/start
func handleCampaignStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	setPaused(false)
	path := getLastUploadedCSV()

	// "Start" resumes the currently active campaign — it never generates a
	// new campaign ID (only a fresh CSV upload does, see handleUpload).
	_, campaignID, ok := getCurrentCampaign(RedisClient)
	if !ok {
		// Should not normally happen (main.go always bootstraps one on
		// first run), but guard against a brand new Redis instance anyway.
		campaignID = newCampaignID()
		if err := setCurrentCampaign(RedisClient, path, campaignID); err != nil {
			writeError(w, http.StatusInternalServerError, "could not register campaign: "+err.Error())
			return
		}
	}

	go func() {
		if err := loadRecipient(path, RedisClient, campaignID); err != nil {
			log.Println("Campaign start error:", err)
			appendActivityLog("failed", "", "failed to load recipients: "+err.Error())
		}
	}()

	appendActivityLog("info", "", fmt.Sprintf("campaign start requested (%s, campaign %s)", path, campaignID))
	writeJSON(w, http.StatusOK, map[string]string{"message": "campaign started", "source": path, "campaign_id": campaignID})
}

// POST /api/campaign/stop
func handleCampaignStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	setPaused(true)
	appendActivityLog("info", "", "campaign paused")
	writeJSON(w, http.StatusOK, map[string]string{"message": "campaign paused"})
}

// GET /api/campaign/status
func handleCampaignStatus(w http.ResponseWriter, r *http.Request) {
	_, campaignID, _ := getCurrentCampaign(RedisClient)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"paused":      isPaused(),
		"source":      getLastUploadedCSV(),
		"campaign_id": campaignID,
	})
}

// GET /api/template — returns the current email template text (headers +
// body) so the UI can show it in an editable textbox.
func handleGetTemplate(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"template": getEmailTemplateText(),
	})
}

// POST /api/template — body: {"template": "..."}. Saves the new template
// text in memory; every email sent after this uses it. Does not touch the
// queue, workers, retry logic, or rate limiter.
func handleUpdateTemplate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	var body struct {
		Template string `json:"template"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if strings.TrimSpace(body.Template) == "" {
		writeError(w, http.StatusBadRequest, "template cannot be empty")
		return
	}

	// Validate it parses as a text/template before saving, so a bad edit
	// from the UI can't break future sends.
	if _, err := template.New("email").Parse(body.Template); err != nil {
		writeError(w, http.StatusBadRequest, "template syntax error: "+err.Error())
		return
	}

	setEmailTemplateText(body.Template)
	appendActivityLog("info", "", "email template updated from dashboard")

	writeJSON(w, http.StatusOK, map[string]string{"message": "template saved"})
}

// GET /api/attachment — returns the currently configured attachment, if any.
func handleGetAttachment(w http.ResponseWriter, r *http.Request) {
	_, filename := getAttachment()
	writeJSON(w, http.StatusOK, map[string]string{"filename": filename})
}

// POST /api/attachment (multipart/form-data, field name "file") — saves the
// file to disk and marks it as the attachment to include on every email
// sent from now on. Does not touch the queue, workers, retry logic, or the
// rate limiter — only helper.go's sendEmail() consults it, and only to
// decide whether to wrap the message as multipart/mixed.
func handleUploadAttachment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}

	if err := r.ParseMultipartForm(20 << 20); err != nil { // 20MB max
		writeError(w, http.StatusBadRequest, "could not parse upload: "+err.Error())
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing 'file' field")
		return
	}
	defer file.Close()

	if err := os.MkdirAll(attachmentsDir, 0755); err != nil {
		writeError(w, http.StatusInternalServerError, "could not create attachments dir")
		return
	}

	safeName := fmt.Sprintf("%d-%s", time.Now().Unix(), filepath.Base(header.Filename))
	destPath := filepath.Join(attachmentsDir, safeName)

	dest, err := os.Create(destPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not save file")
		return
	}
	defer dest.Close()

	if _, err := io.Copy(dest, file); err != nil {
		writeError(w, http.StatusInternalServerError, "could not write file")
		return
	}

	setAttachment(destPath, header.Filename)
	appendActivityLog("info", "", fmt.Sprintf("attachment set: %s", header.Filename))

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":  "attachment saved",
		"filename": header.Filename,
	})
}

// DELETE /api/attachment — removes the configured attachment; new emails go
// out without one again.
func handleRemoveAttachment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "use DELETE")
		return
	}
	clearAttachment()
	appendActivityLog("info", "", "attachment removed")
	writeJSON(w, http.StatusOK, map[string]string{"message": "attachment removed"})
}

// GET /api/logs
func handleLogs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"entries": recentActivity(100),
	})
}

// StartServer boots the HTTP API on its own goroutine. Called additively
// from main.go - it does not replace or reorder any existing startup step.
func StartServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", withCORS(handleStatus))
	mux.HandleFunc("/api/stats", withCORS(handleStats))
	mux.HandleFunc("/api/upload", withCORS(handleUpload))
	mux.HandleFunc("/api/campaign/start", withCORS(handleCampaignStart))
	mux.HandleFunc("/api/campaign/stop", withCORS(handleCampaignStop))
	mux.HandleFunc("/api/campaign/status", withCORS(handleCampaignStatus))
	mux.HandleFunc("/api/logs", withCORS(handleLogs))
	mux.HandleFunc("/api/template", withCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			handleGetTemplate(w, r)
			return
		}
		handleUpdateTemplate(w, r)
	}))
	mux.HandleFunc("/api/attachment", withCORS(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetAttachment(w, r)
		case http.MethodDelete:
			handleRemoveAttachment(w, r)
		default:
			handleUploadAttachment(w, r)
		}
	}))

	port := getPort()
	fmt.Println("HTTP API listening on :" + port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Println("HTTP server error:", err)
	}
}
