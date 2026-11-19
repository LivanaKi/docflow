// Package domain holds the core business model of the conversion service.
// It has no dependencies on transport, storage or messaging.
package domain

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

var (
	ErrNotFound          = errors.New("conversion not found")
	ErrInvalidInput      = errors.New("invalid input")
	ErrInvalidTransition = errors.New("invalid status transition")
)

// map з порожньою структурою — ідіоматичний "set" у Go:
// struct{} займає 0 байт, нам важливі лише ключі.
var supportedFormats = map[string]struct{}{
	"pdf":  {},
	"docx": {},
	"jpeg": {},
	"txt":  {},
	"png":  {},
}

type Conversion struct {
	ID             string
	IdempotencyKey string
	SourceURL      string
	TargetFormat   string
	Status         Status
	ResultURL      string
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewConversion validates the input and returns a conversion in the pending state.
func NewConversion(id, idempotencyKey, sourceURL, targetFormat string, now time.Time) (Conversion, error) {
	addressURL, err := url.ParseRequestURI(sourceURL)
	if err != nil || (addressURL.Scheme != "https" && addressURL.Scheme != "http") || addressURL.Host == "" {
		return Conversion{}, fmt.Errorf("%w: source_url must be an absolute http(s) URL", ErrInvalidInput)
	}

	targetFormat = strings.ToLower(targetFormat)
	targetFormat = strings.TrimSpace(targetFormat)

	if _, ok := supportedFormats[targetFormat]; !ok {
		return Conversion{}, fmt.Errorf("%w: unsupported target format %q", ErrInvalidInput, targetFormat)
	}

	convert := Conversion{
		ID:             id,
		IdempotencyKey: idempotencyKey,
		SourceURL:      sourceURL,
		TargetFormat:   targetFormat,
		Status:         StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	return convert, nil
}

// Start moves a pending conversion to processing.
func (c *Conversion) Start(now time.Time) error {
	return c.transition(StatusPending, StatusProcessing, now)
}

// Complete marks a processing conversion as successfully finished.
func (c *Conversion) Complete(resultURL string, now time.Time) error {
	err := c.transition(StatusProcessing, StatusCompleted, now)
	if err != nil {
		return err
	}

	c.ResultURL = resultURL
	return nil
}

// Fail marks a processing conversion as failed with the given reason.
func (c *Conversion) Fail(reason string, now time.Time) error {
	err := c.transition(StatusProcessing, StatusFailed, now)
	if err != nil {
		return err
	}

	c.Error = reason
	return nil
}

// transition is the only place where the status changes.
func (c *Conversion) transition(from, to Status, now time.Time) error {
	if c.Status != from {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, c.Status, to)
	}

	c.Status = to
	c.UpdatedAt = now
	return nil
}
