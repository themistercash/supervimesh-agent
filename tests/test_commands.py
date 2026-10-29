# SPDX-License-Identifier: Apache-2.0
import json
from datetime import datetime, timedelta, timezone

from supervimesh_agent.commands import CommandHandler

NOW = datetime(2026, 10, 28, 10, 15, tzinfo=timezone.utc)


def cmd(name: str, args: dict | None = None, expires: datetime = NOW + timedelta(minutes=5)) -> bytes:
    body = {"schema_version": 1, "request_id": "r-1", "cmd": name, "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}
    if args is not None:
        body["args"] = args
    return json.dumps(body).encode()


async def _noop() -> None:
    pass


async def test_ping_ok(cfg):
    assert await CommandHandler(cfg, _noop).handle(cmd("ping"), NOW) == ("r-1", True, None)


async def test_expired_command_ignored(cfg):
    called = []

    async def refresh() -> None:
        called.append(1)

    res = await CommandHandler(cfg, refresh).handle(cmd("refresh_status", expires=NOW - timedelta(seconds=1)), NOW)
    assert res == ("r-1", False, "expired") and not called


async def test_non_whitelisted_command_refused(cfg):
    rid, ok, _ = await CommandHandler(cfg, _noop).handle(cmd("reboot"), NOW)
    assert rid == "r-1" and not ok


async def test_garbage_has_no_request_id(cfg):
    assert (await CommandHandler(cfg, _noop).handle(b"rm -rf /", NOW))[0] is None


async def test_update_config_only_non_sensitive(cfg):
    h = CommandHandler(cfg, _noop)
    assert (await h.handle(cmd("update_config", {"status_min_interval_min": 40, "status_max_interval_min": 90}), NOW))[1]
    assert cfg.polling.status_min_interval_min == 40
    rid, ok, err = await h.handle(cmd("update_config", {"managed_repeaters": []}), NOW)
    assert not ok and "managed_repeaters" in err
    rid, ok, _ = await h.handle(cmd("update_config", {"status_min_interval_min": 1}), NOW)
    assert not ok and cfg.polling.status_min_interval_min == 40  # budget radio protégé


async def test_run_test_validates_args_and_delegates(cfg):
    calls = []

    async def run(test_id: str, target: str, request_id: str) -> str | None:
        calls.append((test_id, target))
        return None

    h = CommandHandler(cfg, _noop, run)
    assert await h.handle(cmd("run_test", {"test_id": "t-42", "target": "a1b2c3d4e5f6"}), NOW) == ("r-1", True, None)
    assert (await h.handle(cmd("run_test", {"test_id": "t;rm", "target": "a1b2c3d4e5f6"}), NOW))[1] is False
    assert calls == [("t-42", "a1b2c3d4e5f6")]


async def test_simulator_run_test_conforms_to_contract():
    import jsonschema, json
    from pathlib import Path
    from supervimesh_agent.collectors import envelope
    from supervimesh_agent.simulator import SimulatedRadio

    radio = SimulatedRadio()
    await radio.connect()
    key = next(iter(radio.nodes))
    out = await radio.run_test(key[:12], 5)
    assert out.delivered and out.latency_ms
    msg = envelope(out.ts, test_id="t-1", request_id="r-1", target=key[:12], delivered=out.delivered,
                   latency_ms=out.latency_ms, path=out.path, error=out.error)
    from conftest import schemas_root

    schema = schemas_root() / "schemas" / "test_result.json"
    jsonschema.validate(msg, json.loads(schema.read_text()))
