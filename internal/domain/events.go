package domain

// TopicConversionRequested is the Kafka topic for new conversion jobs.
const TopicConversionRequested = "docflow.conversion.requested"

// ConversionRequested is published when a new conversion is created.
// It carries only the ID: the worker reads the current state from the database.
type ConversionRequested struct {
	ConversionID string `json:"conversion_id"`
}

// OutboxMessage is an event stored in the same transaction as the business data
// and published to Kafka later (transactional outbox pattern).
type OutboxMessage struct {
	Topic   string
	Key     string
	Payload []byte
}
