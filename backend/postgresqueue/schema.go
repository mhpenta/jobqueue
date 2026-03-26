package postgresqueue

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

//go:embed schema/schema/001_queue.sql
var schemaSQL string

type schemaExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// SchemaSQL returns the canonical Postgres schema used by this backend.
func SchemaSQL() string {
	return schemaSQL
}

// EnsureSchema applies the canonical Postgres schema for this backend.
func EnsureSchema(ctx context.Context, execer schemaExecer) error {
	if execer == nil {
		return errors.New("execer is required")
	}
	if _, err := execer.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply postgresqueue schema: %w", err)
	}
	return nil
}
