package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewConversion(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		sourceURL  string
		format     string
		wantErr    error // nil — очікуємо успіх
		wantFormat string
	}{
		{name: "valid", sourceURL: "https://example.com/a.docx", format: "pdf", wantFormat: "pdf"},
		{name: "relative url", sourceURL: "/a.docx", format: "pdf", wantErr: ErrInvalidInput},
		{name: "format with spaces and uppercase letters", sourceURL: "https://example.com/a.docx", format: " DOCX ", wantFormat: "docx"},
		{name: "unsupported scheme", sourceURL: "ftp://example.com/a.docx", format: "pdf", wantErr: ErrInvalidInput},
		{name: "empty URL", sourceURL: "", format: "pdf", wantErr: ErrInvalidInput},
		{name: "unsupported format", sourceURL: "https://example.com/a.docx", format: "exe", wantErr: ErrInvalidInput},
		{name: "jpeg is supported", sourceURL: "https://example.com/a.png", format: "jpeg", wantFormat: "jpeg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConversion("id-1", "key-1", tt.sourceURL, tt.format, now)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.Status != StatusPending {
				t.Errorf("status = %s, want %s", c.Status, StatusPending)
			}
			if c.TargetFormat != tt.wantFormat {
				t.Errorf("format = %s, want %s", c.TargetFormat, tt.wantFormat)
			}
		})
	}
}

func TestConversionLifecycle(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c, err := NewConversion("id-1", "key-1", "https://example.com/a.docx", "pdf", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.Complete("https://result", now)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Complete from pending: expected ErrInvalidTransition, got %v", err)
	}

	err = c.Start(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = c.Start(now)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Start second from pending: expected ErrInvalidTransition, got %v", err)
	}

	err = c.Complete("https://result", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c.Status != StatusCompleted {
		t.Errorf("status = %s, want %s", c.Status, StatusCompleted)
	}

	if c.ResultURL != "https://result" {
		t.Errorf("result_url = %s, want %s", c.ResultURL, "https://result")
	}
}

func TestConversionFail(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t.Run("fail from processing", func(t *testing.T) {
		c, err := NewConversion("id-1", "key-1", "https://example.com/a.docx", "pdf", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = c.Start(now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = c.Fail("timeout", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if c.Status != StatusFailed {
			t.Errorf("status = %s, want %s", c.Status, StatusFailed)
		}

		if c.Error != "timeout" {
			t.Errorf("error = %s, want %s", c.Error, "timeout")
		}
	})

	t.Run("fail after complete is rejected", func(t *testing.T) {
		c, err := NewConversion("id-1", "key-1", "https://example.com/a.docx", "pdf", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = c.Start(now)
		if err != nil {
			t.Fatalf("unexpected error second start: %v", err)
		}

		err = c.Complete("https://result", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = c.Fail("timeout", now)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("Fail after Complete: expected ErrInvalidTransition, got %v", err)
		}

		if c.Status != StatusCompleted {
			t.Errorf("status = %s, want %s", c.Status, StatusCompleted)
		}

		if c.Error != "" {
			t.Errorf("error = %s, want %s", c.Error, "")
		}
	})
}
