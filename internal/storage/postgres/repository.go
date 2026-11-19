// Package postgres implements the conversion repository on top of PostgreSQL.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/LivanaKi/docflow/internal/domain"
)

// go:embed вбудовує файл у бінарник під час компіляції.
//
//go:embed schema.sql
var schema string

// Migrate applies the schema. It is idempotent (CREATE ... IF NOT EXISTS).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// Перелік колонок в одному місці, щоб не повторювати у кожному запиті.
const columns = `id, idempotency_key, source_url, target_format, status, result_url, error, created_at, updated_at`

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// scanConversion reads one row into domain.Conversion.
// pgx.Row — це і результат pool.QueryRow, і tx.QueryRow, тому функція працює з обома.
func scanConversion(row pgx.Row) (domain.Conversion, error) {
	var (
		c      domain.Conversion
		status string
	)

	err := row.Scan(&c.ID, &c.IdempotencyKey, &c.SourceURL, &c.TargetFormat, &status,
		&c.ResultURL, &c.Error, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, domain.ErrNotFound
	}

	if err != nil {
		return c, err
	}

	c.Status = domain.Status(status)
	return c, nil
}

func (r *Repository) Get(ctx context.Context, id string) (domain.Conversion, error) {
	return scanConversion(r.pool.QueryRow(ctx, `SELECT `+columns+` FROM conversions WHERE id = $1`, id))
}

// CreateWithEvent stores the conversion and its outbox event in one transaction.
// If a conversion with the same idempotency key exists, it is returned instead
// and no new event is created.
func (r *Repository) CreateWithEvent(ctx context.Context, c domain.Conversion, msg domain.OutboxMessage) (domain.Conversion, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Conversion{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	// Крок А: спробувати вставити задачу (готово)
	created, err := scanConversion(tx.QueryRow(ctx,
		`INSERT INTO conversions (`+columns+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING `+columns,
		c.ID, c.IdempotencyKey, c.SourceURL, c.TargetFormat, string(c.Status),
		c.ResultURL, c.Error, c.CreatedAt, c.UpdatedAt,
	))

	// Крок Б: ключ уже існує → RETURNING повернув 0 рядків → scanConversion дав ErrNotFound
	if errors.Is(err, domain.ErrNotFound) {
		existing, err := scanConversion(tx.QueryRow(ctx, `SELECT `+columns+` FROM conversions WHERE idempotency_key = $1`, c.IdempotencyKey))
		if err != nil {
			return domain.Conversion{}, fmt.Errorf("get by idempotency key: %w", err)
		}
		return existing, tx.Commit(ctx)
	}
	if err != nil {
		return domain.Conversion{}, fmt.Errorf("insert conversion: %w", err)
	}

	// Крок В: записати подію в outbox у ТІЙ САМІЙ транзакції
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`, msg.Topic, msg.Key, msg.Payload)
	if err != nil {
		return domain.Conversion{}, fmt.Errorf("insert outbox: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return domain.Conversion{}, fmt.Errorf("commit: %w", err)
	}

	return created, nil
}
