package migrations

import (
	"context"
	"embed"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose/v3"
)

//go:embed sql/*.sql
var embedMigrations embed.FS

// Up применяет все неприменённые миграции.
func Up(ctx context.Context, db *sqlx.DB) error {
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	if err := goose.UpContext(ctx, db.DB, "sql"); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}

	return nil
}

// Down откатывает последнюю миграцию. Используется только в разработке.
func Down(ctx context.Context, db *sqlx.DB) error {
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	if err := goose.DownContext(ctx, db.DB, "sql"); err != nil {
		return fmt.Errorf("goose down: %w", err)
	}

	return nil
}
