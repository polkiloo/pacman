package postgres

import "database/sql"

// Client is a PostgreSQL SQL client used by the PACMAN integration layer.
type Client struct {
	db *sql.DB
}

// Connect opens a SQL client for the PostgreSQL instance at the given address.
func Connect(address string) (*Client, error) {
	db, err := openDB("postgres", connectionString(address))
	if err != nil {
		return nil, err
	}

	return &Client{db: db}, nil
}

// Close releases the underlying SQL client resources.
func (client *Client) Close() error {
	if client == nil || client.db == nil {
		return nil
	}

	return client.db.Close()
}
