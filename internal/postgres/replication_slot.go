package postgres

import (
	"context"
	"fmt"
	"strings"
)

const ensurePhysicalReplicationSlotSQL = `
select case
	when pg_is_in_recovery() then false
	when exists (
		select 1
		from pg_replication_slots
		where slot_name = $1 and slot_type = 'physical'
	) then true
	else (pg_create_physical_replication_slot($1)).slot_name = $1
end
`

// EnsurePhysicalReplicationSlot creates the named physical slot on a writable
// PostgreSQL server, or succeeds when that physical slot already exists.
func EnsurePhysicalReplicationSlot(ctx context.Context, address, user, password, slotName string) error {
	address = strings.TrimSpace(address)
	slotName = strings.TrimSpace(slotName)
	if address == "" {
		return fmt.Errorf("replication slot primary address is required")
	}
	if slotName == "" {
		return fmt.Errorf("replication slot name is required")
	}

	connection := connectionString(address)
	if connection == "" {
		return fmt.Errorf("invalid replication slot primary address %q", address)
	}
	connection += " dbname=" + quoteConnectionValue("postgres")
	if user = strings.TrimSpace(user); user != "" {
		connection += " user=" + quoteConnectionValue(user)
	}
	if password != "" {
		connection += " password=" + quoteConnectionValue(password)
	}

	db, err := openDB("postgres", connection)
	if err != nil {
		return fmt.Errorf("connect to replication slot primary %s: %w", address, err)
	}
	client := &Client{db: db}
	defer client.Close()

	return client.EnsurePhysicalReplicationSlot(ctx, slotName)
}

// EnsurePhysicalReplicationSlot creates an idempotent physical slot through
// an existing PostgreSQL connection.
func (client *Client) EnsurePhysicalReplicationSlot(ctx context.Context, slotName string) error {
	if client == nil || client.db == nil {
		return fmt.Errorf("replication slot PostgreSQL client is required")
	}

	var ensured bool
	if err := client.db.QueryRowContext(ctx, ensurePhysicalReplicationSlotSQL, strings.TrimSpace(slotName)).Scan(&ensured); err != nil {
		return fmt.Errorf("ensure physical replication slot %s: %w", slotName, err)
	}
	if !ensured {
		return fmt.Errorf("cannot ensure physical replication slot %s on a standby", slotName)
	}

	return nil
}
