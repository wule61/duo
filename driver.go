package duo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

type ExecContextQuery interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type ExecQuery interface {
	Exec(ctx context.Context, query string, args, v any) error
	Query(ctx context.Context, query string, args, v any) error
}

type Conn struct {
	ExecContextQuery
}

func (c Conn) Exec(ctx context.Context, query string, args, v any) error {
	argsv, ok := args.([]any)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect []any for args", v)
	}

	switch v := v.(type) {
	case nil:
		if _, err := c.ExecContext(ctx, query, argsv...); err != nil {
			return err
		}
	case *sql.Result:
		res, err := c.ExecContext(ctx, query, argsv...)
		if err != nil {
			return err
		}
		*v = res
	default:
		return fmt.Errorf("dialect/sql: invalid type %T. expect *sql.Result", v)
	}
	return nil
}

func (c Conn) Query(ctx context.Context, query string, args, v any) error {
	vr, ok := v.(*Rows)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect *sql.Rows", v)
	}
	argsv, ok := args.([]any)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect []any for args", args)
	}

	rows, err := c.QueryContext(ctx, query, argsv...)
	if err != nil {
		return err
	}

	*vr = Rows{rows}
	return nil
}

type Driver struct {
	Conn
	dialect string
}

func Open(driver, source string) (*Driver, error) {
	db, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	return &Driver{Conn{db}, driver}, nil
}

func OpenDB(driver string, db *sql.DB) (*Driver, error) {
	return &Driver{Conn{db}, driver}, nil
}

func (d Driver) DB() *sql.DB {
	return d.ExecContextQuery.(*sql.DB)
}

type Tx struct {
	ExecContextQuery
	driver.Tx
}

func (d *Driver) Tx(ctx context.Context) (*Tx, error) {
	return d.BeginTx(ctx, nil)
}

func (d *Driver) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.DB().BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		ExecContextQuery: Conn{tx},
		Tx:               tx,
	}, nil
}

func (d *Driver) Close() error {
	return d.DB().Close()
}

type Rows struct {
	ColumnScanner
}

type ColumnScanner interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...any) error
}
