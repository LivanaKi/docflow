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
		return domain.Conversion{}, domain.ErrNotFound
	}

	if err != nil {
		return domain.Conversion{}, err
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

// UpdateStatus saves c only if its stored status still equals from
// (optimistic concurrency). If another process changed the status first,
// it returns domain.ErrInvalidTransition.
func (r *Repository) UpdateStatus(ctx context.Context, c domain.Conversion, from domain.Status) error {
	tag, err := r.pool.Exec(ctx, `UPDATE conversions SET status = $1, result_url = $2, error = $3, updated_at = $4 
                   WHERE id = $5 AND status = $6`, string(c.Status), c.ResultURL, c.Error, c.UpdatedAt, c.ID, string(from))
	if err != nil {
		return fmt.Errorf("update conversion: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrInvalidTransition
	}

	return nil
}

// PublishOutbox locks up to limit unpublished messages, passes them to publish
// and marks them as published in the same transaction.
//
// FOR UPDATE SKIP LOCKED lets several relay instances run in parallel without
// taking the same rows. If publish succeeds but the commit fails, messages are
// sent again on the next run, so delivery is at-least-once.
func (r *Repository) PublishOutbox(ctx context.Context, limit int,
	publish func(context.Context, []domain.OutboxMessage) error) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Крок 1: вибрати і заблокувати пачку
	rows, err := tx.Query(ctx,
		`SELECT id, topic, key, payload FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}

	var (
		ids  []int64
		msgs []domain.OutboxMessage
	)

	// Крок 2: прочитати рядки
	for rows.Next() {
		var (
			id  int64
			msg domain.OutboxMessage
		)

		if err := rows.Scan(&id, &msg.Topic, &msg.Key, &msg.Payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox: %w", err)
		}

		ids = append(ids, id)
		msgs = append(msgs, msg)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}

	// Крок 3: нічого відправляти — виходимо
	if len(msgs) == 0 {
		return 0, nil
	}

	// Крок 4: відправити в Kafka (функція прийшла параметром)
	if err := publish(ctx, msgs); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}

	// Крок 5: позначити відправленими
	_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}

	// Крок 6: зафіксувати
	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	return len(msgs), nil
}
