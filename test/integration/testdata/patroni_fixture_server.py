import copy
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PROFILE = os.environ.get("PATRONI_FIXTURE_PROFILE", "primary")

CONFIG = {
    "ttl": 30,
    "loop_wait": 10,
    "retry_timeout": 10,
    "maximum_lag_on_failover": 1048576,
    "postgresql": {
        "use_slots": True,
        "use_pg_rewind": True,
        "parameters": {
            "hot_standby": "on",
            "wal_level": "hot_standby",
            "max_wal_senders": 5,
            "max_replication_slots": 5,
            "max_connections": "100",
        },
    },
}

PROFILES = {
    "primary": {
        "name": "patroni-primary",
        "role": "primary",
        "leader": True,
        "standby_leader": False,
        "sync": False,
        "quorum": False,
        "async": False,
        "lag": 0,
        "tags": {"clonefrom": True},
    },
    "sync_replica": {
        "name": "patroni-sync",
        "role": "replica",
        "leader": False,
        "standby_leader": False,
        "sync": True,
        "quorum": True,
        "async": False,
        "lag": 0,
        "tags": {"clonefrom": True},
    },
    "async_replica": {
        "name": "patroni-async",
        "role": "replica",
        "leader": False,
        "standby_leader": False,
        "sync": False,
        "quorum": False,
        "async": True,
        "lag": 1048576,
        "tags": {"clonefrom": False},
    },
    "standby_leader": {
        "name": "patroni-standby-leader",
        "role": "standby_leader",
        "leader": True,
        "standby_leader": True,
        "sync": False,
        "quorum": False,
        "async": False,
        "lag": 0,
        "tags": {"clonefrom": True},
    },
}

STATE = {
    "scheduled_switchover": None,
    "scheduled_restart": None,
}


def parse_lag(value):
    if value is None:
        return None
    value = value.strip().lower()
    multipliers = {
        "kb": 1024,
        "mb": 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
    }
    if value.isdigit():
        return int(value)
    for suffix, multiplier in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[:-len(suffix)])) * multiplier
    raise ValueError("invalid lag value")


def merge_patch(target, patch):
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
            continue
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge_patch(target[key], value)
            continue
        target[key] = value
    return target


def node_status():
    profile = PROFILES[PROFILE]
    return {
        "state": "running",
        "role": profile["role"],
        "postmaster_start_time": "2024-08-28T19:39:26Z",
        "server_version": 170004,
        "timeline": 5,
        "tags": copy.deepcopy(profile["tags"]),
        "database_system_identifier": "7408277255830290455",
        "pending_restart": True,
        "pending_restart_reason": {
            "max_connections": {"old_value": "100", "new_value": "101"}
        },
        "dcs_last_seen": 1724874545,
        "xlog": {
            "location": 2197818976,
            "received_location": 2197818976,
            "replayed_location": 2197818976,
            "replayed_timestamp": "2024-08-28T19:40:00Z",
            "paused": False,
        },
        "replication": [
            {
                "usename": "replicator",
                "application_name": "patroni-sync",
                "client_addr": "10.89.0.6",
                "state": "streaming",
                "sync_state": "sync",
                "sync_priority": 1,
            }
        ],
        "patroni": {
            "version": "4.0.0",
            "scope": "batman",
            "name": profile["name"],
        },
    }


def cluster_document():
    document = {
        "scope": "batman",
        "members": [
            {
                "name": "patroni-primary",
                "role": "leader",
                "state": "running",
                "api_url": "http://10.89.0.5:8008/patroni",
                "host": "10.89.0.5",
                "port": 5432,
                "timeline": 5,
                "tags": {"clonefrom": True},
                "lag": 0,
                "lsn": "0/5000000",
            },
            {
                "name": "patroni-sync",
                "role": "sync_standby",
                "state": "running",
                "api_url": "http://10.89.0.6:8008/patroni",
                "host": "10.89.0.6",
                "port": 5433,
                "timeline": 5,
                "tags": {"clonefrom": True},
                "receive_lag": 0,
                "receive_lsn": "0/4000060",
                "replay_lag": 0,
                "replay_lsn": "0/4000060",
                "lag": 0,
                "lsn": "0/4000060",
            },
            {
                "name": "patroni-async",
                "role": "replica",
                "state": "running",
                "api_url": "http://10.89.0.7:8008/patroni",
                "host": "10.89.0.7",
                "port": 5434,
                "timeline": 5,
                "tags": {"clonefrom": False},
                "receive_lag": 1048576,
                "receive_lsn": "0/3000060",
                "replay_lag": 1048576,
                "replay_lsn": "0/3000060",
                "lag": 1048576,
                "lsn": "0/3000060",
            },
        ],
    }
    if STATE["scheduled_switchover"] is not None:
        document["scheduled_switchover"] = copy.deepcopy(STATE["scheduled_switchover"])
    return document


def history_document():
    return [
        [1, 25623960, "no recovery target specified", "2019-09-23T16:57:57+02:00"],
        [2, 25624344, "no recovery target specified", "2019-09-24T09:22:33+02:00"],
        [3, 25624752, "no recovery target specified", "2019-09-24T09:26:15+02:00"],
        [4, 50331856, "no recovery target specified", "2019-09-24T09:35:52+02:00"],
    ]


def known_tag_filters(query):
    return {key: values[0] for key, values in query.items() if key not in {"lag", "replication_state", "mode"}}


def tag_filters_match(profile, query):
    filters = known_tag_filters(query)
    for key, expected in filters.items():
        actual = profile["tags"].get(key)
        if isinstance(actual, bool):
            actual = "true" if actual else "false"
        elif actual is None:
            actual = ""
        else:
            actual = str(actual).lower()
        if actual != expected.lower():
            return False
    return True


def probe_status(path, query):
    profile = PROFILES[PROFILE]
    lag_limit = parse_lag(query.get("lag", [None])[0]) if "lag" in query else None
    replication_state = query.get("replication_state", [None])[0]
    lag_ok = lag_limit is None or profile["lag"] <= lag_limit

    if path in ("/", "/primary", "/read-write"):
        return 200 if profile["role"] == "primary" else 503
    if path == "/leader":
        return 200 if profile["leader"] else 503
    if path in ("/standby-leader", "/standby_leader"):
        return 200 if profile["standby_leader"] else 503
    if path == "/replica":
        if profile["role"] != "replica" or not lag_ok or not tag_filters_match(profile, query):
            return 503
        if replication_state is not None and replication_state != "streaming":
            return 503
        return 200
    if path == "/read-only":
        if not lag_ok or not tag_filters_match(profile, query):
            return 503
        return 200 if profile["role"] in ("primary", "replica", "standby_leader") else 503
    if path in ("/synchronous", "/sync", "/read-only-sync", "/quorum", "/read-only-quorum"):
        return 200 if profile["sync"] or profile["quorum"] else 503
    if path in ("/asynchronous", "/async"):
        return 200 if profile["async"] and lag_ok else 503
    if path == "/health":
        return 200
    if path == "/liveness":
        return 200
    if path == "/readiness":
        if profile["leader"]:
            return 200
        return 200 if lag_ok else 503
    return None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _json_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        payload = self.rfile.read(length)
        return json.loads(payload.decode("utf-8"))

    def _send_json(self, status, payload, head_only=False):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_text(self, status, payload, head_only=False):
        body = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_options(self, allow):
        self.send_response(200)
        self.send_header("Allow", allow)
        self.end_headers()

    def _probe(self, head_only=False):
        parsed = urlparse(self.path)
        status = probe_status(parsed.path, parse_qs(parsed.query))
        if status is None:
            return False
        self._send_json(status, node_status(), head_only=head_only)
        return True

    def do_HEAD(self):
        if self._probe(head_only=True):
            return
        self.send_response(404)
        self.end_headers()

    def do_OPTIONS(self):
        parsed = urlparse(self.path)
        if probe_status(parsed.path, {}) is not None:
            self._send_options("GET, HEAD, OPTIONS")
            return
        allow = {
            "/patroni": "GET, OPTIONS",
            "/metrics": "GET, OPTIONS",
            "/cluster": "GET, OPTIONS",
            "/history": "GET, OPTIONS",
            "/config": "GET, PATCH, PUT, OPTIONS",
            "/switchover": "POST, DELETE, OPTIONS",
            "/failover": "POST, OPTIONS",
            "/restart": "POST, DELETE, OPTIONS",
            "/reload": "POST, OPTIONS",
            "/reinitialize": "POST, OPTIONS",
        }.get(parsed.path)
        if allow is None:
            self.send_response(404)
            self.end_headers()
            return
        self._send_options(allow)

    def do_GET(self):
        if self._probe():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/patroni":
            self._send_json(200, node_status())
            return
        if parsed.path == "/metrics":
            self._send_text(200, "patroni_postgres_running 1\npatroni_primary 1\n")
            return
        if parsed.path == "/cluster":
            self._send_json(200, cluster_document())
            return
        if parsed.path == "/history":
            self._send_json(200, history_document())
            return
        if parsed.path == "/config":
            self._send_json(200, CONFIG)
            return
        self.send_response(404)
        self.end_headers()

    def do_PATCH(self):
        if urlparse(self.path).path != "/config":
            self.send_response(404)
            self.end_headers()
            return
        patch = self._json_body()
        merge_patch(CONFIG, patch)
        self._send_json(200, CONFIG)

    def do_PUT(self):
        if urlparse(self.path).path != "/config":
            self.send_response(404)
            self.end_headers()
            return
        body = self._json_body()
        CONFIG.clear()
        CONFIG.update(body)
        self._send_json(200, CONFIG)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/switchover":
            body = self._json_body()
            if "leader" not in body:
                self._send_text(400, "Switchover could not be scheduled: leader is required")
                return
            if "scheduled_at" in body:
                STATE["scheduled_switchover"] = {
                    "at": body["scheduled_at"],
                    "from": body["leader"],
                    "to": body.get("candidate"),
                }
                self._send_text(202, "Switchover scheduled")
                return
            candidate = body.get("candidate", "patroni-sync")
            self._send_text(200, f'Successfully switched over to "{candidate}"')
            return
        if parsed.path == "/failover":
            body = self._json_body()
            if "candidate" not in body:
                self._send_text(400, "Failover could not be initiated: candidate is required")
                return
            if "leader" in body:
                self._send_text(200, f'Successfully switched over to "{body["candidate"]}"')
                return
            self._send_text(200, f'Successfully failed over to "{body["candidate"]}"')
            return
        if parsed.path == "/restart":
            body = self._json_body()
            if "schedule" in body:
                STATE["scheduled_restart"] = body["schedule"]
                self._send_text(202, "Restart scheduled")
                return
            self._send_text(200, "Restart executed")
            return
        if parsed.path == "/reload":
            self._send_text(200, "Reload requested")
            return
        if parsed.path == "/reinitialize":
            if PROFILE == "primary":
                self._send_text(503, "Reinitialize is not possible on the primary")
                return
            self._json_body()
            self._send_text(200, "Reinitialize requested")
            return
        self.send_response(404)
        self.end_headers()

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path == "/switchover":
            if STATE["scheduled_switchover"] is None:
                self._send_text(404, "No pending scheduled switchover")
                return
            STATE["scheduled_switchover"] = None
            self._send_text(200, "Scheduled switchover deleted")
            return
        if parsed.path == "/restart":
            if STATE["scheduled_restart"] is None:
                self._send_text(404, "No pending scheduled restart")
                return
            STATE["scheduled_restart"] = None
            self._send_text(200, "Scheduled restart deleted")
            return
        self.send_response(404)
        self.end_headers()


HTTPServer(("0.0.0.0", 8008), Handler).serve_forever()
