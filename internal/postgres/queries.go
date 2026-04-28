package postgres

const queryHealthSQL = `
select current_setting('server_version_num')::integer
`

const observationTimelineSQL = `
case
	when local.in_recovery then greatest(
		coalesce(nullif(recovery.min_recovery_end_timeline, 0), 0),
		coalesce((select max(received_tli) from pg_stat_wal_receiver), 0),
		checkpoint.timeline_id
	)
	else ('x' || substr(pg_walfile_name(local.write_lsn::pg_lsn), 1, 8))::bit(32)::bigint
end
`

const queryObservationSQL = `
with local as (
	select
		pg_is_in_recovery() as in_recovery,
		current_setting('server_version_num')::integer as server_version,
		pg_postmaster_start_time() as postmaster_start_at,
		coalesce((select bool_or(pending_restart) from pg_settings), false) as pending_restart,
		case
			when pg_is_in_recovery() then null
			else pg_current_wal_lsn()::text
		end as write_lsn,
		case
			when pg_is_in_recovery() then pg_last_wal_receive_lsn()::text
			else pg_current_wal_flush_lsn()::text
		end as flush_lsn,
		pg_last_wal_receive_lsn()::text as receive_lsn,
		pg_last_wal_replay_lsn()::text as replay_lsn,
		pg_last_xact_replay_timestamp() as replay_timestamp,
		pg_database_size(current_database())::bigint as database_size_bytes
)
select
	local.in_recovery,
	local.server_version,
	local.postmaster_start_at,
	local.pending_restart,
	system.system_identifier::text,
	` + observationTimelineSQL + `,
	coalesce(local.write_lsn, ''),
	coalesce(local.flush_lsn, ''),
	coalesce(local.receive_lsn, ''),
	coalesce(local.replay_lsn, ''),
	local.replay_timestamp,
	local.database_size_bytes,
	case
		when local.receive_lsn is not null and local.replay_lsn is not null
			then pg_wal_lsn_diff(local.receive_lsn::pg_lsn, local.replay_lsn::pg_lsn)::bigint
		else 0
	end
from local
cross join pg_control_system() as system
cross join pg_control_checkpoint() as checkpoint
cross join pg_control_recovery() as recovery
`
