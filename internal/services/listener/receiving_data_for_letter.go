package listener

import (
	"context"
	"db_listener/internal/config"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func GetMessage(id string, cfg *config.Config) ([]byte, error) {
	dsn, err := cfg.Db.DSN()
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(context.Background())

	query := `
        SELECT row_to_json(t)
		FROM (
			SELECT
				"to_address",
				"subject",
				"body_html",
				encode("zip_bytes", 'base64') AS zip_bytes,
				encode("zip_sha256", 'hex') AS zip_sha256
			FROM "email_outbox"
			WHERE "id" = $1 AND "status" != 'sented'
		) t;
    `

	var jsonData []byte
	err = conn.QueryRow(context.Background(), query, id).Scan(&jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to get query: %w", err)
	}

	return jsonData, nil
}
