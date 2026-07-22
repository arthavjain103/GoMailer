package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// This file adds small pieces of shared, in-memory state needed to power the
// new dashboard (pause/resume + a recent-activity feed). It does not change
// how the CSV producer, Redis queues, workers, retry logic, or rate limiter
// behave - it only observes them.
// ---------------------------------------------------------------------------

// paused controls the "Stop/Pause sending" dashboard control. Workers check
// this before picking up new work (see waitWhilePaused in consumer.go).
var paused atomic.Bool

func setPaused(v bool) {
	paused.Store(v)
}

func isPaused() bool {
	return paused.Load()
}

// waitWhilePaused blocks (without discarding the in-flight recipient) until
// the campaign is resumed.
func waitWhilePaused() {
	for isPaused() {
		time.Sleep(200 * time.Millisecond)
	}
}

// lastUploadedCSV tracks the most recently uploaded recipients file so
// "Start Campaign" knows what to load. Defaults to the original sample file
// so the existing behaviour (go run . ⇒ loads dummy_emails.csv) is untouched
// unless the user uploads their own CSV from the UI.
var (
	lastUploadedMu  sync.RWMutex
	lastUploadedCSV = "dummy_emails.csv"
)

func setLastUploadedCSV(path string) {
	lastUploadedMu.Lock()
	defer lastUploadedMu.Unlock()
	lastUploadedCSV = path
}

func getLastUploadedCSV() string {
	lastUploadedMu.RLock()
	defer lastUploadedMu.RUnlock()
	return lastUploadedCSV
}

// emailTemplateText holds the current email template (headers + body) as
// plain text. It defaults to whatever is on disk in email.tmpl (set from
// main.go at startup), so behaviour is unchanged unless the user edits and
// saves a new template from the dashboard's "Template" page.
var (
	emailTemplateMu   sync.RWMutex
	emailTemplateText = ""
)

func setEmailTemplateText(text string) {
	emailTemplateMu.Lock()
	defer emailTemplateMu.Unlock()
	emailTemplateText = text
}

func getEmailTemplateText() string {
	emailTemplateMu.RLock()
	defer emailTemplateMu.RUnlock()
	return emailTemplateText
}

// lastAttachmentPath/lastAttachmentName track an optional file to attach to
// every outgoing email (e.g. a resume/PDF), uploaded from the dashboard.
// Empty path means "no attachment" — the exact pre-existing plain-text send
// path is used in that case.
const attachmentsDir = "attachments"

var (
	attachmentMu       sync.RWMutex
	lastAttachmentPath = ""
	lastAttachmentName = ""
)

// setAttachment records the on-disk path and original filename of the
// uploaded attachment.
func setAttachment(path, filename string) {
	attachmentMu.Lock()
	defer attachmentMu.Unlock()
	lastAttachmentPath = path
	lastAttachmentName = filename
}

// getAttachment returns the on-disk path and original filename of the
// currently configured attachment. Both are empty if none is set.
func getAttachment() (path, filename string) {
	attachmentMu.RLock()
	defer attachmentMu.RUnlock()
	return lastAttachmentPath, lastAttachmentName
}

func clearAttachment() {
	attachmentMu.Lock()
	defer attachmentMu.Unlock()
	lastAttachmentPath = ""
	lastAttachmentName = ""
}

// ActivityEntry is one row in the "Live Monitoring" feed.
type ActivityEntry struct {
	Time    time.Time `json:"time"`
	Status  string    `json:"status"` // sent | retry | failed | info
	Email   string    `json:"email,omitempty"`
	Message string    `json:"message"`
}

const maxActivityEntries = 200

var (
	activityMu  sync.RWMutex
	activityLog = make([]ActivityEntry, 0, maxActivityEntries)
)

// appendActivityLog records an event for the dashboard's live feed. Purely
// additive/observational - never affects queue or retry behaviour.
func appendActivityLog(status, email, message string) {
	activityMu.Lock()
	defer activityMu.Unlock()

	activityLog = append(activityLog, ActivityEntry{
		Time:    time.Now(),
		Status:  status,
		Email:   email,
		Message: message,
	})

	if len(activityLog) > maxActivityEntries {
		activityLog = activityLog[len(activityLog)-maxActivityEntries:]
	}
}

// recentActivity returns the most recent entries, newest first.
func recentActivity(limit int) []ActivityEntry {
	activityMu.RLock()
	defer activityMu.RUnlock()

	n := len(activityLog)
	if limit <= 0 || limit > n {
		limit = n
	}

	out := make([]ActivityEntry, limit)
	for i := 0; i < limit; i++ {
		out[i] = activityLog[n-1-i]
	}
	return out
}
