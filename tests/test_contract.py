# SPDX-License-Identifier: Apache-2.0
"""Tests de contrat : les exemples partagés et les messages produits par l'agent respectent les schémas JSON."""
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import PassiveCollector, StatusPoller, TelemetryPoller
from supervimesh_agent.config import EventLogConfig, Sensor
from supervimesh_agent.radio import AdvertSeen, ChannelMessage, PacketHeard
from supervimesh_agent.schemas import MODELS
from supervimesh_agent.simulator import SimulatedRadio

from conftest import schemas_root

ROOT = schemas_root()
pytestmark = pytest.mark.skipif(not ROOT.exists(), reason="schémas MQTT partagés absents")


def load(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def json_schema(name: str):
    return load(ROOT / "schemas" / f"{name}.json")


VALID = sorted((ROOT / "examples" / "valid").glob("*.json")) if ROOT.exists() else []
INVALID = sorted((ROOT / "examples" / "invalid").glob("*.json")) if ROOT.exists() else []


@pytest.mark.parametrize("path", VALID, ids=lambda p: p.stem)
def test_valid_examples_accepted(path):
    MODELS[path.stem].model_validate(load(path))
    jsonschema.validate(load(path), json_schema(path.stem))


@pytest.mark.parametrize("path", INVALID, ids=lambda p: p.stem)
def test_invalid_examples_rejected(path):
    case = load(path)
    with pytest.raises(ValidationError):
        MODELS[case["type"]].model_validate(case["message"])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(case["message"], json_schema(case["type"]))


@pytest.mark.parametrize("name", list(MODELS))
def test_pydantic_matches_json_schema_fields(name):
    js = json_schema(name)
    fields = {f.alias or n: f for n, f in MODELS[name].model_fields.items()}
    assert set(fields) == set(js["properties"])
    assert {k for k, f in fields.items() if f.is_required()} == set(js["required"])


async def test_agent_output_conforms_to_json_schemas(cfg):
    box = Outbox(":memory:")
    radio = SimulatedRadio(rng=random.Random(3))
    await radio.connect()
    cfg = cfg.model_copy(update={
        "event_log": EventLogConfig(channels=["Organisation"]),
        "sensors": [Sensor(public_key="d4e5f6a1b2c3")],
    })
    col = PassiveCollector(cfg, box, lambda: radio.self_key)
    col.handle(PacketHeard(snr=6.0, rssi=-100, last_hop="a1"))
    now = datetime.now(timezone.utc)
    for text in ("Ravito km 18: Besoin d'eau", "Serre-file: POS 45.2,5.7", "PC Course: SOS coureur 45.2051, 5.7398"):
        col.handle(ChannelMessage(1, "Organisation", text, now, now, 5.0))
    col.handle(ChannelMessage(0, "Public", "Rando38: Salut", now, now, 5.0))  # canal non autorisé
    col.handle(AdvertSeen("a1b2c3d4e5f6" + "0" * 52, "Relais", "repeater", None, None, hops=None))
    col.flush_links(datetime.now(timezone.utc))
    await StatusPoller(cfg, box, radio).poll_once(cfg.managed_repeaters[0])
    assert await TelemetryPoller(cfg, box, radio).poll_once(cfg.sensors[0])
    by_suffix = {
        "node/advert": "node_advert", "link": "link", "node/status": "node_status", "event_log": "event_log",
        "node/telemetry": "node_telemetry",
    }
    msgs = box.peek()
    assert len(msgs) == 7
    assert sorted(m.payload["kind"] for m in msgs if m.topic.endswith("/event_log")) == ["message", "position", "sos"]
    for m in msgs:
        suffix = m.topic.split("/", 4)[4]
        jsonschema.validate(m.payload, json_schema(by_suffix[suffix]))
