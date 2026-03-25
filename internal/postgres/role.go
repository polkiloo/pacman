package postgres

import (
	"context"

	"github.com/polkiloo/pacman/internal/cluster"
)

// QueryRole detects the current PostgreSQL member role at the given address.
func QueryRole(ctx context.Context, address string) (cluster.MemberRole, error) {
	client, err := Connect(address)
	if err != nil {
		return cluster.MemberRoleUnknown, err
	}
	defer client.Close()

	return client.QueryRole(ctx)
}

// QueryRole detects the current PostgreSQL member role through the connected
// PostgreSQL client.
func (client *Client) QueryRole(ctx context.Context) (cluster.MemberRole, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return cluster.MemberRoleUnknown, err
	}

	return observation.Role, nil
}
