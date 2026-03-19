package cluster

import "fmt"

// Generation identifies an operator-driven desired-state revision for the
// cluster. It should advance when the accepted cluster spec changes.
type Generation int64

// Validate reports whether the generation is suitable for use as cluster
// truth.
func (generation Generation) Validate() error {
	if generation < 0 {
		return ErrClusterGenerationNegative
	}

	return nil
}

// IsZero reports whether the generation is still at the initial revision.
func (generation Generation) IsZero() bool {
	return generation == 0
}

func (generation Generation) String() string {
	return fmt.Sprintf("%d", generation)
}

// Epoch identifies the currently authoritative topology term for the cluster.
// It should advance when PACMAN publishes a new primary authority decision.
type Epoch int64

// Validate reports whether the epoch is suitable for use as cluster truth.
func (epoch Epoch) Validate() error {
	if epoch < 0 {
		return ErrClusterEpochNegative
	}

	return nil
}

// IsZero reports whether the epoch is still at the initial topology term.
func (epoch Epoch) IsZero() bool {
	return epoch == 0
}

func (epoch Epoch) String() string {
	return fmt.Sprintf("%d", epoch)
}
