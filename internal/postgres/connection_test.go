package postgres

import (
	"database/sql"
	"errors"
	"testing"
)

func TestConnectReturnsClient(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{}), nil
	})
	defer restore()

	client, err := Connect("127.0.0.1:5432")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if client == nil || client.db == nil {
		t.Fatal("expected connected client")
	}
}

func TestConnectReturnsOpenError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	client, err := Connect("127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected open error")
	}

	if client != nil {
		t.Fatalf("expected nil client on open error, got %+v", client)
	}
}

func TestClientCloseAllowsNilReceiver(t *testing.T) {
	var client *Client

	if err := client.Close(); err != nil {
		t.Fatalf("close nil client: %v", err)
	}
}
