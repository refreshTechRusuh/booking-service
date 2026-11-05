// Package db menyediakan interface IPool untuk memudahkan testing tanpa DB nyata.
package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IPool adalah subset pgxpool.Pool yang dipakai package ini.
// *pgxpool.Pool memenuhi interface ini secara otomatis di production.
// pgxmock.NewPool() memenuhi interface ini di test.
type IPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}
