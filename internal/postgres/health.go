package postgres

import "context"

// Health describes the latest PostgreSQL health-query result.
type Health struct {
	ServerVersion int
}

// QueryHealth executes a lightweight SQL health query against PostgreSQL.
func QueryHealth(ctx context.Context, address string) (Health, error) {
	client, err := Connect(address)
	if err != nil {
		return Health{}, err
	}
	defer client.Close()

	return client.QueryHealth(ctx)
}

// QueryHealth executes a lightweight SQL health query through the connected
// PostgreSQL client.
func (client *Client) QueryHealth(ctx context.Context) (Health, error) {
	var health Health
	err := client.db.QueryRowContext(ctx, queryHealthSQL).Scan(&health.ServerVersion)
	return health, err
}
