# SPDX-License-Identifier: Apache-2.0
"""Relevé de la télémétrie des capteurs MeshCore (CayenneLPP) et publication node_telemetry."""
import json
import math
import random
from datetime import datetime, timezone

import jsonschema
import pytest
from pydantic import ValidationError

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import TelemetryPoller, telemetry_message
from supervimesh_agent.config import AgentConfig, RadioConfig, Sensor
from supervimesh_agent.radio import MAX_READINGS, MeshCoreRadio, normalize_readings
from supervimesh_agent.simulator import SimulatedRadio

from conftest import schemas_root

SENSOR_KEY = "d4e5f6a1b2c3" + "0" * 52  # clé publique MeshCore fictive, pas un secret  gitleaks:allow
NOW = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)


def node_telemetry_schema():
    path = schemas_root() / "schemas" / "node_telemetry.json"
    if not path.exists():
        pytest.skip("schémas MQTT partagés absents")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def sensor_cfg(cfg):
    return cfg.model_copy(update={"sensors": [Sensor(public_key=SENSOR_KEY[:12].upper(), name="Capteur Col")]})


# --- Normalisation -------------------------------------------------------------------------------------------

def test_normalize_keeps_numbers_and_gps_object():
    raw = [
        {"channel": 1, "type": "voltage", "value": 3.92},
        {"channel": 2, "type": "Temperature", "value": 3.5},
        {"channel": 2, "type": "humidity", "value": 81},
        {"channel": 3, "type": "gps", "value": {"latitude": 45.12, "longitude": 6.21, "altitude": 980}},
    ]
    out = normalize_readings(raw)
    assert [r["type"] for r in out] == ["voltage", "temperature", "humidity", "gps"]
    assert out[3]["value"] == {"latitude": 45.12, "longitude": 6.21, "altitude": 980}


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, "21.5", True, None, [1.0, 2.0, 3.0], {}])
def test_normalize_rejects_non_finite_or_non_numeric_values(value):
    assert normalize_readings([{"channel": 1, "type": "temperature", "value": value}]) == []


@pytest.mark.parametrize("value", [{"x": math.nan}, {"x": "1"}, {"x": True}, {"k" * 17: 1.0}])
def test_normalize_rejects_bad_composite_values(value):
    assert normalize_readings([{"channel": 3, "type": "gps", "value": value}]) == []


@pytest.mark.parametrize("item", [
    {"channel": 256, "type": "temperature", "value": 1.0},
    {"channel": -1, "type": "temperature", "value": 1.0},
    {"channel": True, "type": "temperature", "value": 1.0},
    {"channel": "1", "type": "temperature", "value": 1.0},
    {"channel": 1, "type": 103, "value": 1.0},
    {"channel": 1, "type": "température °C", "value": 1.0},
    {"channel": 1, "type": "x" * 33, "value": 1.0},
    {"channel": 1, "value": 1.0},
    "temperature",
])
def test_normalize_rejects_bad_channel_or_type(item):
    assert normalize_readings([item]) == []


def test_normalize_limits_to_32_readings():
    raw = [{"channel": i, "type": "generic sensor", "value": i} for i in range(40)]
    out = normalize_readings(raw)
    assert len(out) == MAX_READINGS == 32 and out[-1]["channel"] == 31


def test_normalize_ignores_non_list():
    assert normalize_readings(None) == [] and normalize_readings({"channel": 1}) == []


def test_empty_telemetry_publishes_nothing():
    assert telemetry_message(SENSOR_KEY, [], NOW) is None
    assert telemetry_message(SENSOR_KEY, [{"channel": 1, "type": "temperature", "value": math.nan}], NOW) is None


def test_telemetry_message_conforms_to_json_schema():
    raw = [
        {"channel": 1, "type": "voltage", "value": 3.92},
        {"channel": 2, "type": "temperature", "value": "?"},  # écartée
        {"channel": 3, "type": "gps", "value": {"latitude": 45.12, "longitude": 6.21, "altitude": 980}},
    ]
    payload = telemetry_message(SENSOR_KEY[:12].upper(), raw, NOW)
    jsonschema.validate(payload, node_telemetry_schema())
    assert payload["public_key"] == SENSOR_KEY[:12] and len(payload["readings"]) == 2
    json.dumps(payload, allow_nan=False)  # JSON strict : aucun NaN ne peut sortir


# --- Configuration ---------------------------------------------------------------------------------------------

def test_sensor_config_defaults_and_password_hidden():
    full = AgentConfig.model_validate({
        "identity": {"org": "org-1", "site": "site-1", "gateway": "gw-1"},
        "sensors": [{"public_key": SENSOR_KEY, "password": "mot-de-passe-capteur"}, {"public_key": SENSOR_KEY[:12]}],
    })
    with_pwd, without = full.sensors
    assert with_pwd.interval_min == 15 and with_pwd.password == "mot-de-passe-capteur" and without.password is None
    assert "mot-de-passe-capteur" not in repr(full) and "mot-de-passe-capteur" not in repr(with_pwd)


@pytest.mark.parametrize("sensor", [
    {"public_key": SENSOR_KEY, "interval_min": 4},  # budget radio
    {"public_key": "abc"},                          # clé trop courte
    {"public_key": "zz" * 6},                       # pas de l'hexadécimal
])
def test_sensor_config_rejects_bad_entries(sensor):
    with pytest.raises(ValidationError):
        Sensor.model_validate(sensor)


# --- Relevé ----------------------------------------------------------------------------------------------------

async def test_poller_publishes_simulated_telemetry(sensor_cfg):
    box = Outbox(":memory:")
    radio = SimulatedRadio(rng=random.Random(4))
    await radio.connect()
    assert await TelemetryPoller(sensor_cfg, box, radio).poll_once(sensor_cfg.sensors[0])
    [msg] = box.peek()
    assert msg.topic == "mv/org-1/site-1/gw-1/node/telemetry" and msg.qos == 1
    jsonschema.validate(msg.payload, node_telemetry_schema())
    assert {r["type"] for r in msg.payload["readings"]} == {"voltage", "temperature", "humidity"}


async def test_poller_publishes_nothing_when_disconnected_or_unknown(sensor_cfg):
    box = Outbox(":memory:")
    radio = SimulatedRadio()
    poller = TelemetryPoller(sensor_cfg, box, radio)
    assert not await poller.poll_once(sensor_cfg.sensors[0])  # radio pas encore connectée
    await radio.connect()
    assert not await poller.poll_once(Sensor(public_key="eeeeeeeeeeee"))  # capteur inconnu
    assert box.pending_count() == 0


async def test_radio_error_never_crashes_agent(sensor_cfg):
    class BrokenRadio(SimulatedRadio):
        async def request_telemetry(self, sensor, timeout_s):
            raise TimeoutError("radio muette")

    box = Outbox(":memory:")
    assert not await TelemetryPoller(sensor_cfg, box, BrokenRadio()).poll_once(sensor_cfg.sensors[0])
    assert box.pending_count() == 0


def test_telemetry_delay_respects_interval_with_jitter(sensor_cfg):
    poller = TelemetryPoller(sensor_cfg, Outbox(":memory:"), SimulatedRadio(), rng=random.Random(0))
    delays = [poller.next_delay_s(sensor_cfg.sensors[0]) for _ in range(200)]
    assert min(delays) >= 15 * 60 and max(delays) <= 18 * 60
    assert len(set(delays)) > 100


# --- MeshCore réel (meshcore_py simulé) -------------------------------------------------------------------------

class FakeCommands:
    def __init__(self, login_ok=True, lpp=None):
        self.login_ok, self.lpp, self.logins, self.requests = login_ok, lpp, [], 0

    async def send_login_sync(self, contact, pwd, timeout=0):
        self.logins.append(pwd)
        return object() if self.login_ok else None

    async def req_telemetry_sync(self, contact, timeout=0):
        self.requests += 1
        return self.lpp


class FakeMeshCore:
    def __init__(self, commands):
        self.commands = commands

    def get_contact_by_key_prefix(self, prefix):
        return {"public_key": SENSOR_KEY} if SENSOR_KEY.startswith(prefix.lower()) else None


def meshcore_radio(commands):
    radio = MeshCoreRadio(RadioConfig())
    radio._mc = FakeMeshCore(commands)
    return radio


async def test_meshcore_telemetry_without_password_skips_login():
    lpp = [{"channel": 1, "type": "voltage", "value": 4.1}]
    cmds = FakeCommands(lpp=lpp)
    assert await meshcore_radio(cmds).request_telemetry(Sensor(public_key=SENSOR_KEY), 5) == lpp
    assert cmds.logins == [] and cmds.requests == 1


async def test_meshcore_telemetry_logs_in_when_password_set():
    cmds = FakeCommands(lpp=[])
    await meshcore_radio(cmds).request_telemetry(Sensor(public_key=SENSOR_KEY, password="invite"), 5)
    assert cmds.logins == ["invite"] and cmds.requests == 1


async def test_meshcore_telemetry_refused_login_or_unknown_sensor():
    cmds = FakeCommands(login_ok=False, lpp=[{"channel": 1, "type": "voltage", "value": 4.1}])
    radio = meshcore_radio(cmds)
    assert await radio.request_telemetry(Sensor(public_key=SENSOR_KEY, password="faux"), 5) is None
    assert await radio.request_telemetry(Sensor(public_key="eeeeeeeeeeee"), 5) is None
    assert cmds.requests == 0
