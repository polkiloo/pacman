#include "postgres.h"

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

#if PG_VERSION_NUM < 170000 || PG_VERSION_NUM >= 180000
#error "pacman_agent currently supports PostgreSQL 17.x only"
#endif

static volatile sig_atomic_t pacman_got_sigterm = false;

static char *pacman_node_name = NULL;
static char *pacman_node_role = "data";
static char *pacman_api_address = "0.0.0.0:8080";
static char *pacman_control_address = "0.0.0.0:9090";
static char *pacman_helper_path = "pacmand";
static char *pacman_postgres_data_dir = NULL;
static char *pacman_postgres_bin_dir = NULL;
static char *pacman_postgres_listen_address = "127.0.0.1";
static int pacman_postgres_port = 5432;
static char *pacman_cluster_name = NULL;
static char *pacman_initial_primary = NULL;
static char *pacman_seed_addresses = NULL;
static char *pacman_expected_members = NULL;

static const int pacman_helper_restart_delay_seconds = 1;
static const int pacman_helper_stop_wait_attempts = 50;
static const useconds_t pacman_helper_stop_wait_interval_usec = 100000;

PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void pacman_agent_main(Datum main_arg);

static void pacman_agent_sigterm(SIGNAL_ARGS);
static void pacman_define_gucs(void);
static void pacman_register_worker(void);
static pid_t pacman_start_helper(void);
static void pacman_export_snapshot_environment(void);
static void pacman_export_string_setting(const char *key, const char *value);
static void pacman_export_int_setting(const char *key, int value);
static void pacman_bridge_connection_environment(void);
static void pacman_unset_problematic_environment(void);
static void pacman_close_inherited_fds(void);
static void pacman_stop_helper(pid_t helper_pid);
static void pacman_check_helper_exit(pid_t *helper_pid, time_t *next_restart_at);

PGDLLEXPORT void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pacman_agent must be loaded via shared_preload_libraries")));
	}

	pacman_define_gucs();
	pacman_register_worker();
}

PGDLLEXPORT void
pacman_agent_main(Datum main_arg)
{
	pid_t helper_pid = -1;
	time_t next_restart_at = 0;

	BackgroundWorkerUnblockSignals();
	pqsignal(SIGTERM, pacman_agent_sigterm);

	ereport(LOG,
			(errmsg("pacman_agent background worker started"),
			 errdetail("node_name=\"%s\" node_role=\"%s\" api_address=\"%s\" control_address=\"%s\" data_dir=\"%s\" cluster_name=\"%s\"",
					   pacman_node_name ? pacman_node_name : "",
					   pacman_node_role ? pacman_node_role : "",
					   pacman_api_address ? pacman_api_address : "",
					   pacman_control_address ? pacman_control_address : "",
					   pacman_postgres_data_dir ? pacman_postgres_data_dir : "",
					   pacman_cluster_name ? pacman_cluster_name : "")));

	if (pacman_node_name == NULL || pacman_node_name[0] == '\0')
	{
		ereport(WARNING,
				(errmsg("%s is not configured", "pacman.node_name"),
				 errhint("Set pacman.node_name in postgresql.conf before relying on pacman_agent.")));
	}

	helper_pid = pacman_start_helper();
	if (helper_pid < 0)
		next_restart_at = time(NULL) + pacman_helper_restart_delay_seconds;

	while (!pacman_got_sigterm)
	{
		int rc;

		pacman_check_helper_exit(&helper_pid, &next_restart_at);
		if (!pacman_got_sigterm && helper_pid < 0 && time(NULL) >= next_restart_at)
			helper_pid = pacman_start_helper();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   200L,
					   0);
		ResetLatch(MyLatch);

		if ((rc & WL_POSTMASTER_DEATH) != 0)
		{
			pacman_stop_helper(helper_pid);
			proc_exit(1);
		}
	}

	pacman_stop_helper(helper_pid);
	ereport(LOG, (errmsg("pacman_agent background worker stopping")));
	proc_exit(0);
}

static void
pacman_agent_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	pacman_got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
pacman_define_gucs(void)
{
	DefineCustomStringVariable("pacman.node_name",
							   "Local PACMAN node name.",
							   "Maps to PACMAN node.name.",
							   &pacman_node_name,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.node_role",
							   "Static PACMAN node role.",
							   "Maps to PACMAN node.role.",
							   &pacman_node_role,
							   "data",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.api_address",
							   "PACMAN HTTP API listen address.",
							   "Maps to PACMAN node.apiAddress.",
							   &pacman_api_address,
							   "0.0.0.0:8080",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.control_address",
							   "PACMAN control-plane advertise address.",
							   "Maps to PACMAN node.controlAddress.",
							   &pacman_control_address,
							   "0.0.0.0:9090",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.helper_path",
							   "PACMAN helper binary used by the background worker.",
							   "Defaults to pacmand on the PATH and may be overridden for packaging or tests.",
							   &pacman_helper_path,
							   "pacmand",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.postgres_data_dir",
							   "Local PostgreSQL data directory for PACMAN.",
							   "Maps to PACMAN postgres.dataDir.",
							   &pacman_postgres_data_dir,
							   DataDir ? DataDir : "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.postgres_bin_dir",
							   "Optional PostgreSQL binary directory for PACMAN.",
							   "Maps to PACMAN postgres.binDir.",
							   &pacman_postgres_bin_dir,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.postgres_listen_address",
							   "Local PostgreSQL TCP listen address for PACMAN probes.",
							   "Maps to PACMAN postgres.listenAddress.",
							   &pacman_postgres_listen_address,
							   "127.0.0.1",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("pacman.postgres_port",
							"Local PostgreSQL TCP port for PACMAN probes.",
							"Maps to PACMAN postgres.port.",
							&pacman_postgres_port,
							5432,
							1,
							65535,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pacman.cluster_name",
							   "PACMAN bootstrap cluster name.",
							   "Maps to PACMAN bootstrap.clusterName.",
							   &pacman_cluster_name,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.initial_primary",
							   "PACMAN bootstrap initial primary.",
							   "Maps to PACMAN bootstrap.initialPrimary.",
							   &pacman_initial_primary,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.seed_addresses",
							   "Comma-separated PACMAN bootstrap seed addresses.",
							   "Maps to PACMAN bootstrap.seedAddresses.",
							   &pacman_seed_addresses,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pacman.expected_members",
							   "Comma-separated PACMAN bootstrap expected members.",
							   "Maps to PACMAN bootstrap.expectedMembers.",
							   &pacman_expected_members,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);
}

static void
pacman_register_worker(void)
{
	BackgroundWorker worker;

	MemSet(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;

	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pacman_agent");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pacman_agent_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "PACMAN local agent");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pacman_agent");

	RegisterBackgroundWorker(&worker);
}

static pid_t
pacman_start_helper(void)
{
	pid_t pid = fork();

	if (pid < 0)
	{
		ereport(WARNING,
				(errmsg("failed to fork PACMAN helper process"),
				 errdetail("%m")));
		return -1;
	}

	if (pid == 0)
	{
		char *argv[3];

		pacman_export_snapshot_environment();
		pacman_bridge_connection_environment();
		pacman_unset_problematic_environment();
		pacman_close_inherited_fds();

		argv[0] = pacman_helper_path ? pacman_helper_path : "pacmand";
		argv[1] = "-pgext-env";
		argv[2] = NULL;

		execvp(argv[0], argv);
		fprintf(stderr, "pacman_agent: execvp(%s) failed: %s\n", argv[0], strerror(errno));
		_exit(127);
	}

	ereport(LOG,
			(errmsg("started PACMAN helper process"),
			 errdetail("pid=%d helper_path=\"%s\"",
					   (int) pid,
					   pacman_helper_path ? pacman_helper_path : "")));

	return pid;
}

static void
pacman_export_snapshot_environment(void)
{
	pacman_export_string_setting("PACMAN_PGEXT_NODE_NAME", pacman_node_name);
	pacman_export_string_setting("PACMAN_PGEXT_NODE_ROLE", pacman_node_role);
	pacman_export_string_setting("PACMAN_PGEXT_API_ADDRESS", pacman_api_address);
	pacman_export_string_setting("PACMAN_PGEXT_CONTROL_ADDRESS", pacman_control_address);
	pacman_export_string_setting("PACMAN_PGEXT_POSTGRES_DATA_DIR", pacman_postgres_data_dir);
	pacman_export_string_setting("PACMAN_PGEXT_POSTGRES_BIN_DIR", pacman_postgres_bin_dir);
	pacman_export_string_setting("PACMAN_PGEXT_POSTGRES_LISTEN_ADDRESS", pacman_postgres_listen_address);
	pacman_export_int_setting("PACMAN_PGEXT_POSTGRES_PORT", pacman_postgres_port);
	pacman_export_string_setting("PACMAN_PGEXT_CLUSTER_NAME", pacman_cluster_name);
	pacman_export_string_setting("PACMAN_PGEXT_INITIAL_PRIMARY", pacman_initial_primary);
	pacman_export_string_setting("PACMAN_PGEXT_SEED_ADDRESSES", pacman_seed_addresses);
	pacman_export_string_setting("PACMAN_PGEXT_EXPECTED_MEMBERS", pacman_expected_members);
}

static void
pacman_export_string_setting(const char *key, const char *value)
{
	if (setenv(key, value ? value : "", 1) != 0)
		fprintf(stderr, "pacman_agent: setenv(%s) failed: %s\n", key, strerror(errno));
}

static void
pacman_export_int_setting(const char *key, int value)
{
	char buffer[32];

	snprintf(buffer, sizeof(buffer), "%d", value);
	pacman_export_string_setting(key, buffer);
}

static void
pacman_bridge_connection_environment(void)
{
	const char *value;

	if (getenv("PGDATABASE") == NULL && (value = getenv("POSTGRES_DB")) != NULL)
		pacman_export_string_setting("PGDATABASE", value);
	if (getenv("PGUSER") == NULL && (value = getenv("POSTGRES_USER")) != NULL)
		pacman_export_string_setting("PGUSER", value);
	if (getenv("PGPASSWORD") == NULL && (value = getenv("POSTGRES_PASSWORD")) != NULL)
		pacman_export_string_setting("PGPASSWORD", value);
	if (getenv("PGSSLMODE") == NULL)
		pacman_export_string_setting("PGSSLMODE", "disable");
}

static void
pacman_unset_problematic_environment(void)
{
	unsetenv("PGSYSCONFDIR");
	unsetenv("PGLOCALEDIR");
}

static void
pacman_close_inherited_fds(void)
{
	long max_fd = sysconf(_SC_OPEN_MAX);
	int fd;

	if (max_fd < 0)
		max_fd = 1024;

	for (fd = 3; fd < max_fd; fd++)
		close(fd);
}

static void
pacman_stop_helper(pid_t helper_pid)
{
	int attempt;
	int status;

	if (helper_pid <= 0)
		return;

	ereport(LOG,
			(errmsg("stopping PACMAN helper process"),
			 errdetail("pid=%d", (int) helper_pid)));

	if (kill(helper_pid, SIGTERM) != 0 && errno != ESRCH)
	{
		int save_errno = errno;

		ereport(WARNING,
				(errmsg("failed to signal PACMAN helper process"),
				 errdetail("pid=%d errno=%d", (int) helper_pid, save_errno)));
	}

	for (attempt = 0; attempt < pacman_helper_stop_wait_attempts; attempt++)
	{
		pid_t waited = waitpid(helper_pid, &status, WNOHANG);

		if (waited == helper_pid)
			return;
		if (waited < 0 && errno == ECHILD)
			return;

		usleep(pacman_helper_stop_wait_interval_usec);
	}

	ereport(WARNING,
			(errmsg("PACMAN helper process did not stop after SIGTERM, sending SIGKILL"),
			 errdetail("pid=%d", (int) helper_pid)));
	kill(helper_pid, SIGKILL);
	while (waitpid(helper_pid, &status, 0) < 0 && errno == EINTR)
		continue;
}

static void
pacman_check_helper_exit(pid_t *helper_pid, time_t *next_restart_at)
{
	int status;
	pid_t waited;

	if (helper_pid == NULL || *helper_pid <= 0)
		return;

	waited = waitpid(*helper_pid, &status, WNOHANG);
	if (waited == 0)
		return;

	if (waited < 0)
	{
		if (errno != ECHILD)
		{
			int save_errno = errno;

			ereport(WARNING,
					(errmsg("failed to observe PACMAN helper process"),
					 errdetail("pid=%d errno=%d", (int) *helper_pid, save_errno)));
		}
	}
	else if (WIFEXITED(status))
	{
		ereport(WARNING,
				(errmsg("PACMAN helper process exited"),
				 errdetail("pid=%d exit_code=%d",
						   (int) *helper_pid,
						   WEXITSTATUS(status))));
	}
	else if (WIFSIGNALED(status))
	{
		ereport(WARNING,
				(errmsg("PACMAN helper process exited on signal"),
				 errdetail("pid=%d signal=%d",
						   (int) *helper_pid,
						   WTERMSIG(status))));
	}
	else
	{
		ereport(WARNING,
				(errmsg("PACMAN helper process changed state"),
				 errdetail("pid=%d status=%d", (int) *helper_pid, status)));
	}

	*helper_pid = -1;
	if (next_restart_at != NULL)
		*next_restart_at = time(NULL) + pacman_helper_restart_delay_seconds;
}
