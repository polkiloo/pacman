FROM postgres:17-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3,psycopg3]==4.1.3"

ENV PATH="/opt/patroni/bin:${PATH}"

ENTRYPOINT ["patroni"]
