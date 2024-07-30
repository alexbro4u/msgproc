package postgres

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

type Storage struct {
	db *sql.DB
}

func NewStorage(user, pass, name, host string) (*Storage, error) {
	const op = "internal/storage/postgres.NewStorage"

	db, err := sql.Open("postgres",
		fmt.Sprintf(
			"user=%s"+
				" password=%s"+
				" dbname=%s"+
				" host=%s"+
				" sslmode=disable",
			user,
			pass,
			name,
			host,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveMsg(
	ctx context.Context,
	msg string,
) (int64, error) {
	const op = "internal/storage/postgres.SaveMsg"

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	var finalErr error
	defer func() {
		if p := recover(); p != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				finalErr = fmt.Errorf("%s: %w", op, rollbackErr)
			}
			panic(p)
		} else if finalErr != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				finalErr = fmt.Errorf("%s: %w", op, rollbackErr)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				finalErr = fmt.Errorf("%s: %w", op, commitErr)
			}
		}
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO messages
		    (content)
		VALUES
		    (:content)
		RETURNING id into :id
	`)
	if err != nil {
		finalErr = fmt.Errorf("%s: %w", op, err)
		return 0, finalErr
	}
	defer func() {
		stmtErr := stmt.Close()
		if stmtErr != nil {
			finalErr = fmt.Errorf("%s: %w", op, stmtErr)
		}
	}()

	var msgID int64
	_, err = stmt.ExecContext(
		ctx,
		sql.Named("content", msg),
		sql.Named("id", sql.Out{Dest: &msgID}),
	)
	if err != nil {
		finalErr = fmt.Errorf("%s: %w", op, err)
		return 0, finalErr
	}

	return msgID, nil
}

func (s *Storage) TotalMessages(ctx context.Context) (int64, error) {
	const op = "internal/storage/postgres.TotalMessages"

	var total int64
	err := s.db.QueryRowContext(ctx, `
		SELECT
		    COUNT(*)
		FROM
		    messages
	`).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return total, nil
}

func (s *Storage) MessagesByStatus(ctx context.Context) (map[string]int64, error) {
	const op = "internal/storage/postgres.MessagesByStatus"

	rows, err := s.db.QueryContext(ctx, `
		SELECT
		    status, COUNT(*)
		FROM
		    messages
		GROUP BY status
	`)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer func() {
		rowsErr := rows.Close()
		if rowsErr != nil {
			log.Println(rowsErr)
		}
	}()

	statusCounts := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		statusCounts[status] = count
	}

	return statusCounts, nil
}

func (s *Storage) MessagesLastDay(ctx context.Context) (int64, error) {
	const op = "internal/storage/postgres.MessagesLastDay"

	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT
		    COUNT(*)
		FROM
		    messages
		WHERE 
		    created_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return count, nil
}

func (s *Storage) MessagesUpdatedLastDay(ctx context.Context) (int64, error) {
	const op = "internal/storage/postgres.MessagesUpdatedLastDay"

	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT
		    COUNT(*)
		FROM
		    messages
		WHERE
		    updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return count, nil
}

func (s *Storage) AverageMessageLength(ctx context.Context) (float64, error) {
	const op = "internal/storage/postgres.AverageMessageLength"

	var avgLength float64
	err := s.db.QueryRowContext(ctx, `
		SELECT
		    AVG(LENGTH(content))
		FROM
		    messages
	`).Scan(&avgLength)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return avgLength, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}
