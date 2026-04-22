package postgres

import (
	"strings"
	"testing"
)

func TestObservationTimelineSQLUsesRecoveryAwareSources(t *testing.T) {
	t.Parallel()

	wantParts := []string{
		"recovery.min_recovery_end_timeline",
		"checkpoint.timeline_id",
		"pg_walfile_name(local.write_lsn::pg_lsn)",
	}

	for _, wantPart := range wantParts {
		if !strings.Contains(observationTimelineSQL, wantPart) {
			t.Fatalf("observation timeline SQL %q does not contain %q", observationTimelineSQL, wantPart)
		}
	}

	if !strings.Contains(queryObservationSQL, "cross join pg_control_recovery() as recovery") {
		t.Fatalf("query observation SQL %q does not join pg_control_recovery()", queryObservationSQL)
	}
}
