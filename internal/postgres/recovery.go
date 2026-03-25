package postgres

import "context"

// QueryRecoveryState detects whether PostgreSQL is currently in recovery.
func QueryRecoveryState(ctx context.Context, address string) (bool, error) {
	client, err := Connect(address)
	if err != nil {
		return false, err
	}
	defer client.Close()

	return client.QueryRecoveryState(ctx)
}

// QueryRecoveryState detects whether PostgreSQL is currently in recovery
// through the connected PostgreSQL client.
func (client *Client) QueryRecoveryState(ctx context.Context) (bool, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return false, err
	}

	return observation.InRecovery, nil
}
