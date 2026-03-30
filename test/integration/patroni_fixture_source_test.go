//go:build integration

package integration_test

import _ "embed"

// patroniCompatibilityFixtureServerSource contains the Patroni-compatible
// Python HTTP fixture served by the integration tests.
//
//go:embed testdata/patroni_fixture_server.py
var patroniCompatibilityFixtureServerSource string
