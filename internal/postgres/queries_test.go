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
		"pg_database_size(current_database())::bigint as database_size_bytes",
	}

	for _, wantPart := range wantParts {
		if !strings.Contains(queryObservationSQL, wantPart) {
			t.Fatalf("query observation SQL %q does not contain %q", queryObservationSQL, wantPart)
		}
	}

	if !strings.Contains(queryObservationSQL, "cross join pg_control_recovery() as recovery") {
		t.Fatalf("query observation SQL %q does not join pg_control_recovery()", queryObservationSQL)
	}
}
