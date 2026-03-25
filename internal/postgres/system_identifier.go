package postgres

import "context"

// QuerySystemIdentifier looks up the current PostgreSQL system identifier.
func QuerySystemIdentifier(ctx context.Context, address string) (string, error) {
	client, err := Connect(address)
	if err != nil {
		return "", err
	}
	defer client.Close()

	return client.QuerySystemIdentifier(ctx)
}

// QuerySystemIdentifier looks up the current PostgreSQL system identifier
// through the connected PostgreSQL client.
func (client *Client) QuerySystemIdentifier(ctx context.Context) (string, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return "", err
	}

	return observation.Details.SystemIdentifier, nil
}
