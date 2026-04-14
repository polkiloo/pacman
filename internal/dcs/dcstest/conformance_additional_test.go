package dcstest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/memory"
)

func TestRunCoversAdditionalFailureBranches(t *testing.T) {
	t.Parallel()

	for _, caseName := range []string{
		"bad_get_key",
		"bad_compare_value",
		"bad_list_count",
		"bad_initial_leader",
		"bad_alive_error",
		"bad_watch_put",
	} {
		t.Run(caseName, func(t *testing.T) {
			runConformanceFailureHelper(t, caseName)
		})
	}
}

func TestConformanceExercisesDirectPositiveCases(t *testing.T) {
	t.Parallel()

	const ttl = 40 * time.Millisecond

	testCases := []struct {
		name string
		run  func(t *testing.T, backend dcs.DCS) error
	}{
		{
			name: "get set delete",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
		},
		{
			name: "compare and set",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
		},
		{
			name: "list prefix",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
		},
		{
			name: "campaign leader resign",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
		},
		{
			name: "touch alive ttl",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
		},
		{
			name: "watch event delivery",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := newConformanceCaseBackend(t, "", ttl)
			if err := testCase.run(t, backend); err != nil {
				t.Fatalf("unexpected positive conformance helper error: %v", err)
			}
		})
	}
}

func TestConformanceExercisesDirectNegativeCases(t *testing.T) {
	t.Parallel()

	const ttl = 40 * time.Millisecond

	testCases := []struct {
		name     string
		caseName string
		run      func(t *testing.T, backend dcs.DCS) error
		want     string
	}{
		{
			name:     "bad get key",
			caseName: "bad_get_key",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected key",
		},
		{
			name:     "bad compare value",
			caseName: "bad_compare_value",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected updated value",
		},
		{
			name:     "bad list count",
			caseName: "bad_list_count",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "unexpected listed entry count",
		},
		{
			name:     "bad initial leader",
			caseName: "bad_initial_leader",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "expected no leader before campaigning",
		},
		{
			name:     "bad alive error",
			caseName: "bad_alive_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "alive before touch",
		},
		{
			name:     "bad watch put",
			caseName: "bad_watch_put",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "unexpected put event",
		},
		{
			name:     "bad get backend error",
			caseName: "bad_get_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "get key",
		},
		{
			name:     "bad set backend error",
			caseName: "bad_set_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "set key",
		},
		{
			name:     "bad get value",
			caseName: "bad_get_value",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected value",
		},
		{
			name:     "bad get revision",
			caseName: "bad_get_revision",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected revision",
		},
		{
			name:     "bad get ttl",
			caseName: "bad_get_ttl",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected ttl",
		},
		{
			name:     "bad get after delete",
			caseName: "bad_get_after_delete",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected get-after-delete error",
		},
		{
			name:     "bad delete error",
			caseName: "bad_delete_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "delete key",
		},
		{
			name:     "bad delete missing result",
			caseName: "bad_delete_missing",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected delete-missing error",
		},
		{
			name:     "bad compare seed error",
			caseName: "bad_compare_seed",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "seed key",
		},
		{
			name:     "bad compare backend error",
			caseName: "bad_compare_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "compare-and-set success",
		},
		{
			name:     "bad compare get seeded key",
			caseName: "bad_compare_get_seed_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "get seeded key",
		},
		{
			name:     "bad compare get updated key",
			caseName: "bad_compare_get_updated_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "get updated key",
		},
		{
			name:     "bad compare revision",
			caseName: "bad_compare_revision",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected updated revision",
		},
		{
			name:     "bad stale compare result",
			caseName: "bad_compare_stale_result",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config")
			},
			want: "unexpected stale revision error",
		},
		{
			name:     "bad list seed alpha2",
			caseName: "bad_list_seed_alpha2",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "seed alpha-2",
		},
		{
			name:     "bad list seed status",
			caseName: "bad_list_seed_status",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "seed status key",
		},
		{
			name:     "bad list seed alpha1",
			caseName: "bad_list_seed_alpha1",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "seed alpha-1",
		},
		{
			name:     "bad list backend error",
			caseName: "bad_list_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "list members",
		},
		{
			name:     "bad list keys",
			caseName: "bad_list_keys",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "unexpected listed keys",
		},
		{
			name:     "bad list get error",
			caseName: "bad_list_get_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "get listed key",
		},
		{
			name:     "bad list shared value",
			caseName: "bad_list_shared_value",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseListPrefix(context.Background(), backend)
			},
			want: "detached copies",
		},
		{
			name:     "bad initial leader error",
			caseName: "bad_leader_error_initial",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "initial leader lookup",
		},
		{
			name:     "bad campaign not held",
			caseName: "bad_campaign_not_held",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "expected first candidate to hold leader lease",
		},
		{
			name:     "bad campaign leader",
			caseName: "bad_campaign_leader",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "unexpected leader after campaign",
		},
		{
			name:     "bad campaign term",
			caseName: "bad_campaign_term",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "unexpected initial term",
		},
		{
			name:     "bad leader after campaign error",
			caseName: "bad_leader_error_after_campaign",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "leader after campaign",
		},
		{
			name:     "bad leader after campaign state",
			caseName: "bad_leader_state_after_campaign",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "unexpected leader state after campaign",
		},
		{
			name:     "bad campaign backend error",
			caseName: "bad_campaign_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "campaign leader",
		},
		{
			name:     "bad competing campaign error",
			caseName: "bad_campaign_competing_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseCampaignLeaderResign(context.Background(), backend, ttl)
			},
			want: "campaign competing leader",
		},
		{
			name:     "bad touch error",
			caseName: "bad_touch_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "touch member",
		},
		{
			name:     "bad alive initial true",
			caseName: "bad_alive_initial_true",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "expected untouched member to be reported dead",
		},
		{
			name:     "bad alive after touch error",
			caseName: "bad_alive_after_touch_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "alive after touch",
		},
		{
			name:     "bad alive after touch false",
			caseName: "bad_alive_after_touch_false",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "expected touched member to be reported alive",
		},
		{
			name:     "bad alive during wait error",
			caseName: "bad_alive_during_wait_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "alive during expiry wait",
		},
		{
			name:     "bad alive never expires",
			caseName: "bad_alive_never_expires",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseTouchAliveTTL(context.Background(), backend, ttl)
			},
			want: "expected touched member session to expire",
		},
		{
			name:     "bad watch set error",
			caseName: "bad_watch_set_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "set watched key",
		},
		{
			name:     "bad watch setup error",
			caseName: "bad_watch_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "watch status prefix",
		},
		{
			name:     "bad closed watch channel",
			caseName: "bad_watch_closed",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "watch channel closed",
		},
		{
			name:     "bad watch delete error",
			caseName: "bad_watch_delete_error",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "delete watched key",
		},
		{
			name:     "bad watch delete wait",
			caseName: "bad_watch_delete_wait",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "watch channel closed",
		},
		{
			name:     "bad watch delete event",
			caseName: "bad_watch_delete_event",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "unexpected delete event",
		},
		{
			name:     "bad watch ttl set",
			caseName: "bad_watch_ttl_set",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "set ttl watched key",
		},
		{
			name:     "bad watch ttl wait",
			caseName: "bad_watch_ttl_wait",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "watch channel closed",
		},
		{
			name:     "bad watch ttl put",
			caseName: "bad_watch_ttl_put",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "unexpected ttl put event",
		},
		{
			name:     "bad watch expired wait",
			caseName: "bad_watch_expired_wait",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "watch channel closed",
		},
		{
			name:     "bad watch expired event",
			caseName: "bad_watch_expired_event",
			run: func(t *testing.T, backend dcs.DCS) error {
				t.Helper()
				return exerciseWatchEventDelivery(context.Background(), backend, ttl)
			},
			want: "unexpected expired event",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := newConformanceCaseBackend(t, testCase.caseName, ttl)
			err := testCase.run(t, backend)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("unexpected negative conformance helper error: got %v want substring %q", err, testCase.want)
			}
		})
	}
}

func TestValidateConfigAndOpenBackendHelpers(t *testing.T) {
	t.Parallel()

	if err := validateConfig(Config{}); err == nil || !strings.Contains(err.Error(), "backend constructor") {
		t.Fatalf("unexpected validateConfig missing constructor error: %v", err)
	}

	if err := validateConfig(Config{New: func(*testing.T) dcs.DCS { return &trackingDCS{} }}); err == nil || !strings.Contains(err.Error(), "positive ttl") {
		t.Fatalf("unexpected validateConfig ttl error: %v", err)
	}

	if err := validateConfig(Config{TTL: time.Second, New: func(*testing.T) dcs.DCS { return &trackingDCS{} }}); err != nil {
		t.Fatalf("unexpected validateConfig success error: %v", err)
	}

	_, err := openBackend(t, Config{
		TTL: time.Second,
		New: func(*testing.T) dcs.DCS {
			return &trackingDCS{initializeErr: errors.New("boom")}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected openBackend initialize error: %v", err)
	}
}

func TestWaitForEventSuccessAndExpiryWait(t *testing.T) {
	t.Parallel()

	t.Run("wait for event success", func(t *testing.T) {
		t.Parallel()

		events := make(chan dcs.WatchEvent, 1)
		events <- dcs.WatchEvent{Type: dcs.EventPut, Key: "/pacman/alpha/status/alpha-1", Value: []byte("up")}

		got := waitForEvent(t, events, time.Second)
		if got.Type != dcs.EventPut || got.Key != "/pacman/alpha/status/alpha-1" || string(got.Value) != "up" {
			t.Fatalf("unexpected waitForEvent result: %+v", got)
		}
	})

	t.Run("expiry wait adds half ttl", func(t *testing.T) {
		t.Parallel()

		if got := expiryWait(80 * time.Millisecond); got != 120*time.Millisecond {
			t.Fatalf("unexpected expiry wait: got %s want %s", got, 120*time.Millisecond)
		}
	})

	t.Run("wait for event timeout result", func(t *testing.T) {
		t.Parallel()

		_, err := waitForEventResult(make(chan dcs.WatchEvent), 10*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "timed out waiting for watch event") {
			t.Fatalf("unexpected waitForEventResult timeout: %v", err)
		}
	})
}

func TestConformanceCoverageFailureHelper(t *testing.T) {
	caseName := os.Getenv("PACMAN_DCSTEST_COVERAGE_HELPER_CASE")
	if caseName == "" {
		return
	}

	const ttl = 40 * time.Millisecond

	Run(t, Config{
		TTL: ttl,
		New: func(*testing.T) dcs.DCS {
			return &conformanceCaseDCS{
				caseName: caseName,
				inner: memory.New(memory.Config{
					TTL:           ttl,
					SweepInterval: 5 * time.Millisecond,
				}),
			}
		},
	})
}

func runConformanceFailureHelper(t *testing.T, caseName string) {
	t.Helper()

	command := exec.Command(os.Args[0], "-test.run=^TestConformanceCoverageFailureHelper$")
	command.Env = append(os.Environ(), "PACMAN_DCSTEST_COVERAGE_HELPER_CASE="+caseName)

	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected helper case %q to fail test process, got %v", caseName, err)
	}
}

type conformanceCaseDCS struct {
	caseName      string
	inner         dcs.DCS
	getCalls      int
	setCalls      int
	deleteCalls   int
	compareCalls  int
	campaignCalls int
	leaderCalls   int
	aliveCalls    int
}

func (backend *conformanceCaseDCS) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	backend.getCalls++
	switch backend.caseName {
	case "bad_get_error":
		if backend.getCalls == 1 {
			return dcs.KeyValue{}, errors.New("get failed")
		}
	case "bad_compare_get_seed_error":
		if backend.getCalls == 1 {
			return dcs.KeyValue{}, errors.New("get seeded failed")
		}
	case "bad_compare_get_updated_error":
		if backend.getCalls == 2 {
			return dcs.KeyValue{}, errors.New("get updated failed")
		}
	case "bad_list_get_error":
		if backend.getCalls == 1 {
			return dcs.KeyValue{}, errors.New("get listed failed")
		}
	case "bad_get_after_delete":
		if backend.getCalls == 2 {
			return dcs.KeyValue{Key: key, Value: []byte("ghost"), Revision: 2}, nil
		}
	}

	value, err := backend.inner.Get(ctx, key)
	if err != nil {
		return value, err
	}

	switch backend.caseName {
	case "bad_get_key":
		if backend.getCalls == 1 {
			value.Key = key + "/wrong"
		}
	case "bad_get_value":
		if backend.getCalls == 1 {
			value.Value = []byte("wrong")
		}
	case "bad_get_revision":
		if backend.getCalls == 1 {
			value.Revision = 2
		}
	case "bad_get_ttl":
		if backend.getCalls == 1 {
			value.TTL = time.Second
		}
	case "bad_compare_value":
		if backend.getCalls == 2 {
			value.Value = []byte("wrong")
		}
	case "bad_compare_revision":
		if backend.getCalls == 2 {
			value.Revision = 99
		}
	case "bad_list_shared_value":
		if backend.getCalls == 1 {
			value.Value = []byte("Xirst")
		}
	}

	return value, nil
}

func (backend *conformanceCaseDCS) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	backend.setCalls++
	switch backend.caseName {
	case "bad_set_error":
		if backend.setCalls == 1 {
			return errors.New("set failed")
		}
	case "bad_compare_seed":
		if backend.setCalls == 1 {
			return errors.New("seed failed")
		}
	case "bad_list_seed_alpha2":
		if backend.setCalls == 1 {
			return errors.New("seed alpha-2 failed")
		}
	case "bad_list_seed_status":
		if backend.setCalls == 2 {
			return errors.New("seed status failed")
		}
	case "bad_list_seed_alpha1":
		if backend.setCalls == 3 {
			return errors.New("seed alpha-1 failed")
		}
	case "bad_watch_set_error":
		if backend.setCalls == 1 && len(options) == 0 {
			return errors.New("set failed")
		}
	case "bad_watch_ttl_set":
		if len(options) > 0 {
			return errors.New("ttl set failed")
		}
	}
	return backend.inner.Set(ctx, key, value, options...)
}

func (backend *conformanceCaseDCS) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	backend.compareCalls++
	if backend.caseName == "bad_compare_error" && backend.compareCalls == 1 {
		return errors.New("compare failed")
	}
	if backend.caseName == "bad_compare_stale_result" && backend.compareCalls == 2 {
		return nil
	}
	return backend.inner.CompareAndSet(ctx, key, value, expectedRevision)
}

func (backend *conformanceCaseDCS) Delete(ctx context.Context, key string) error {
	backend.deleteCalls++
	if backend.caseName == "bad_delete_error" && backend.deleteCalls == 1 {
		return errors.New("delete failed")
	}
	if backend.caseName == "bad_delete_missing" && backend.deleteCalls == 2 {
		return nil
	}
	if backend.caseName == "bad_watch_delete_error" && backend.deleteCalls == 1 {
		return errors.New("delete failed")
	}
	return backend.inner.Delete(ctx, key)
}

func (backend *conformanceCaseDCS) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	if backend.caseName == "bad_list_error" {
		return nil, errors.New("list failed")
	}
	values, err := backend.inner.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if backend.caseName == "bad_list_count" && len(values) > 0 {
		return values[:1], nil
	}
	if backend.caseName == "bad_list_keys" && len(values) == 2 {
		values[0], values[1] = values[1], values[0]
	}

	return values, nil
}

func (backend *conformanceCaseDCS) Campaign(ctx context.Context, candidate string) (dcs.LeaderLease, bool, error) {
	backend.campaignCalls++
	switch backend.caseName {
	case "bad_campaign_error":
		if backend.campaignCalls == 1 {
			return dcs.LeaderLease{}, false, errors.New("campaign failed")
		}
	case "bad_campaign_competing_error":
		if backend.campaignCalls == 2 {
			return dcs.LeaderLease{}, false, errors.New("competing campaign failed")
		}
	}

	lease, held, err := backend.inner.Campaign(ctx, candidate)
	if err != nil {
		return lease, held, err
	}

	switch backend.caseName {
	case "bad_campaign_not_held":
		if backend.campaignCalls == 1 {
			return lease, false, nil
		}
	case "bad_campaign_leader":
		if backend.campaignCalls == 1 {
			lease.Leader = "beta-9"
			return lease, held, nil
		}
	case "bad_campaign_term":
		if backend.campaignCalls == 1 {
			lease.Term = 2
			return lease, held, nil
		}
	}

	return lease, held, nil
}

func (backend *conformanceCaseDCS) Leader(ctx context.Context) (dcs.LeaderLease, bool, error) {
	backend.leaderCalls++
	switch backend.caseName {
	case "bad_initial_leader":
		if backend.leaderCalls == 1 {
			return dcs.LeaderLease{Leader: "ghost", Term: 1}, true, nil
		}
	case "bad_leader_error_initial":
		if backend.leaderCalls == 1 {
			return dcs.LeaderLease{}, false, errors.New("leader failed")
		}
	case "bad_leader_error_after_campaign":
		if backend.leaderCalls == 2 {
			return dcs.LeaderLease{}, false, errors.New("leader after campaign failed")
		}
	case "bad_leader_state_after_campaign":
		if backend.leaderCalls == 2 {
			return dcs.LeaderLease{}, false, nil
		}
	case "bad_leader_error_after_resign":
		if backend.leaderCalls == 3 {
			return dcs.LeaderLease{}, false, errors.New("leader after resign failed")
		}
	case "bad_leader_present_after_resign":
		if backend.leaderCalls == 3 {
			return dcs.LeaderLease{Leader: "beta-1", Term: 2}, true, nil
		}
	}

	return backend.inner.Leader(ctx)
}

func (backend *conformanceCaseDCS) Resign(ctx context.Context) error {
	if backend.caseName == "bad_resign_error" {
		return errors.New("resign failed")
	}
	return backend.inner.Resign(ctx)
}

func (backend *conformanceCaseDCS) Touch(ctx context.Context, member string) error {
	if backend.caseName == "bad_touch_error" {
		return errors.New("touch failed")
	}
	return backend.inner.Touch(ctx, member)
}

func (backend *conformanceCaseDCS) Alive(ctx context.Context, member string) (bool, error) {
	backend.aliveCalls++
	switch backend.caseName {
	case "bad_alive_error":
		if backend.aliveCalls == 1 {
			return false, errors.New("alive failed")
		}
	case "bad_alive_initial_true":
		if backend.aliveCalls == 1 {
			return true, nil
		}
	case "bad_alive_after_touch_error":
		if backend.aliveCalls == 2 {
			return false, errors.New("alive after touch failed")
		}
	case "bad_alive_after_touch_false":
		if backend.aliveCalls == 2 {
			return false, nil
		}
	case "bad_alive_during_wait_error":
		if backend.aliveCalls >= 3 {
			return false, errors.New("alive during wait failed")
		}
	case "bad_alive_never_expires":
		if backend.aliveCalls >= 2 {
			return true, nil
		}
	}

	return backend.inner.Alive(ctx, member)
}

func (backend *conformanceCaseDCS) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	if backend.caseName == "bad_watch_error" {
		return nil, errors.New("watch failed")
	}
	if backend.caseName == "bad_watch_closed" {
		events := make(chan dcs.WatchEvent)
		close(events)
		return events, nil
	}

	events, err := backend.inner.Watch(ctx, prefix)
	if err != nil {
		return events, err
	}

	switch backend.caseName {
	case "bad_watch_put",
		"bad_watch_delete_wait",
		"bad_watch_delete_event",
		"bad_watch_ttl_wait",
		"bad_watch_ttl_put",
		"bad_watch_expired_wait",
		"bad_watch_expired_event":
	default:
		return events, nil
	}

	out := make(chan dcs.WatchEvent, 8)
	go func() {
		defer close(out)

		eventIndex := 0
		for event := range events {
			current := event.Clone()
			eventIndex++
			switch backend.caseName {
			case "bad_watch_put":
				if eventIndex == 1 {
					current.Key = prefix + "unexpected"
				}
			case "bad_watch_delete_wait":
				if eventIndex == 1 {
					out <- current
					return
				}
			case "bad_watch_delete_event":
				if eventIndex == 2 {
					current.Type = dcs.EventPut
				}
			case "bad_watch_ttl_wait":
				if eventIndex == 2 {
					out <- current
					return
				}
			case "bad_watch_ttl_put":
				if eventIndex == 3 {
					current.Key = prefix + "unexpected-ttl"
				}
			case "bad_watch_expired_wait":
				if eventIndex == 3 {
					out <- current
					return
				}
			case "bad_watch_expired_event":
				if eventIndex == 4 {
					current.Type = dcs.EventDelete
				}
			}
			out <- current
		}
	}()

	return out, nil
}

func (backend *conformanceCaseDCS) Initialize(ctx context.Context) error {
	return backend.inner.Initialize(ctx)
}

func (backend *conformanceCaseDCS) Close() error {
	return backend.inner.Close()
}

func newConformanceCaseBackend(t *testing.T, caseName string, ttl time.Duration) dcs.DCS {
	t.Helper()

	backend := &conformanceCaseDCS{
		caseName: caseName,
		inner: memory.New(memory.Config{
			TTL:           ttl,
			SweepInterval: 5 * time.Millisecond,
		}),
	}

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize conformance case backend: %v", err)
	}

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close conformance case backend: %v", err)
		}
	})

	return backend
}
