package postgres

import "testing"

func TestSummarizeEmailEvents(t *testing.T) {
	events := []EmailEvent{
		{Status: "queued"},
		{Status: "queued"},
		{Status: "sent"},
		{Status: "retry"},
		{Status: "failed"},
		{Status: "sent"},
	}

	summary := SummarizeEmailEvents(events)

	if summary["queued"] != 2 {
		t.Fatalf("queued expected 2, got %d", summary["queued"])
	}
	if summary["sent"] != 2 {
		t.Fatalf("sent expected 2, got %d", summary["sent"])
	}
	if summary["retry"] != 1 {
		t.Fatalf("retry expected 1, got %d", summary["retry"])
	}
	if summary["failed"] != 1 {
		t.Fatalf("failed expected 1, got %d", summary["failed"])
	}
}
