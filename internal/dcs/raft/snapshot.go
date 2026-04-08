package raft

import (
	"encoding/json"

	hraft "github.com/hashicorp/raft"
)

type snapshot struct {
	state snapshotState
}

func (snapshot *snapshot) Persist(sink hraft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(snapshot.state); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (snapshot *snapshot) Release() {}
