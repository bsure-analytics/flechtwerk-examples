"""Shared integration-tier fixtures for every example.

Each example co-locates its three test tiers under ``examples/<name>/tests/``;
this repo-root conftest gives the integration tier (``pytest -m integration``)
a set of session-scoped Docker containers shared across all examples, with
per-test isolation achieved via unique topic names. It mirrors the convention
in the flechtwerk repo's ``tests/integration/conftest.py`` — the same broker
tuning, the same ``unique_*`` helpers — so the examples read like the
framework's own integration suite.

The container fixtures start lazily and skip cleanly when Docker is
unreachable, so a plain ``pytest`` run (the Docker-free tiers) never touches
Docker. Additional service fixtures (ClickHouse, Mosquitto) are added
alongside the example whose integration tier first needs them.

Run with:
    uv run poe test-integration     # integration tier only
    uv run poe test-all             # every tier
"""
import uuid

import pytest


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    """Start a Kafka container for the whole test session.

    Returns the bootstrap server address (host:port). The broker is tuned for
    single-broker transactional tests: ``__transaction_state``,
    ``__consumer_offsets``, and related internal topics default to
    replication-factor 3, which fails with one broker. We override them to 1
    to match the ephemeral test setup.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.kafka import KafkaContainer

    kafka = (
        KafkaContainer()
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_MIN_INSYNC_REPLICAS", "1")
    )
    with kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture(scope="session")
def clickhouse() -> dict[str, str]:
    """Start a ClickHouse container for the whole test session.

    Returns the HTTP connection info the examples' ClickHouse writers take. A
    dedicated (non-``default``) user is created via ``CLICKHOUSE_USER``, which
    the image makes reachable from any network — unlike the built-in ``default``
    user, which is locked to loopback.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.clickhouse import ClickHouseContainer

    container = ClickHouseContainer(
        "clickhouse/clickhouse-server:25.8",
        username="flechtwerk",
        password="flechtwerk",
        dbname="flechtwerk",
    )
    container.with_exposed_ports(8123, 9000)
    with container:
        yield {
            "base_url": f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8123)}",
            "database": container.dbname,
            "user": container.username,
            "password": container.password,
        }


_MOSQUITTO_CONFIG = """\
listener 1883
protocol mqtt
allow_anonymous true
persistence true
persistence_location /mosquitto/data/
"""


@pytest.fixture(scope="session")
def mosquitto() -> dict[str, object]:
    """Start a Mosquitto broker once per session, mirroring the framework's fixture.

    The config is injected via the container command rather than a host
    bind-mount: bind-mounting a single file fails on Docker-outside-of-Docker
    runners, so `MosquittoContainer.start()` (which always bind-mounts) is
    bypassed in favour of the base `DockerContainer.start()`.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    import shlex

    from testcontainers.core.container import DockerContainer
    from testcontainers.mqtt import MosquittoContainer

    container = MosquittoContainer("eclipse-mosquitto:2.1.2-alpine")
    container.with_exposed_ports(MosquittoContainer.MQTT_PORT)
    container.with_command([
        "sh", "-c",
        f"printf %s {shlex.quote(_MOSQUITTO_CONFIG)} > /tmp/mosquitto.conf && exec mosquitto -c /tmp/mosquitto.conf",
    ])
    try:
        DockerContainer.start(container)  # skip MosquittoContainer.start()'s bind-mount
        container._wait()
        yield {"broker": container.get_container_host_ip(), "port": int(container.get_exposed_port(1883))}
    finally:
        container.stop()


@pytest.fixture
def unique_topic() -> str:
    """Per-test topic name to avoid cross-test contamination."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def unique_changelog_topic() -> str:
    """Per-test changelog topic name."""
    return f"changelog-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def unique_group_id() -> str:
    """Per-test consumer group ID."""
    return f"group-{uuid.uuid4().hex[:12]}"
