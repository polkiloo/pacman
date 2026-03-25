package postgres

import "context"

// QueryWALProgress looks up the latest locally observed PostgreSQL WAL
// positions.
func QueryWALProgress(ctx context.Context, address string) (WALProgress, error) {
	client, err := Connect(address)
	if err != nil {
		return WALProgress{}, err
	}
	defer client.Close()

	return client.QueryWALProgress(ctx)
}

// QueryWALProgress looks up the latest locally observed PostgreSQL WAL
// positions through the connected PostgreSQL client.
func (client *Client) QueryWALProgress(ctx context.Context) (WALProgress, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return WALProgress{}, err
	}

	return observation.WAL, nil
}
