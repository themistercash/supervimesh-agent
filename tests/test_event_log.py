# SPDX-License-Identifier: Apache-2.0
"""Main courante : canaux d'exploitation autorisés, expéditeur, SOS, position, horodatage."""
import asyncio
import random
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import (
    PassiveCollector, event_log_message, find_position, is_sos, split_sender,
)
from supervimesh_agent.config import AgentConfig, EventLogConfig
from supervimesh_agent.radio import ChannelMessage, MeshCoreRadio, RadioConfig, channel_payload_to_message
from supervimesh_agent.schemas import EventLog
from supervimesh_agent.simulator import SimulatedRadio

NOW = datetime(2026, 10, 30, 9, 0, tzinfo=timezone.utc)
ORGA = EventLogConfig(channels=["Organisation"])


def msg(text: str, idx: int = 1, name: str | None = "Organisation", sender_ts: datetime | None = NOW):
    return ChannelMessage(channel_idx=idx, channel_name=name, text=text, sender_ts=sender_ts, received_at=NOW, snr=4.0)


@pytest.mark.parametrize("text, expected", [
    ("Ravito km 18: besoin d'eau", ("Ravito km 18", "besoin d'eau")),
    ("  PC Course :  RAS  ", ("PC Course", "RAS")),             # typographie française « Nom : »
    ("Serre-file: heure: 10h", ("Serre-file", "heure: 10h")),   # coupe au premier « : »
    ("pas de nom ici", (None, "pas de nom ici")),
    (": vide", (None, ": vide")),
    ("x" * 65 + ": trop long", (None, "x" * 65 + ": trop long")),
])
def test_split_sender(text, expected):
    assert split_sender(text) == expected


@pytest.mark.parametrize("content, expected", [
    ("SOS", True), ("sos !", True), ("SOS: coureur à terre", True), ("  Sos coureur", True),
    ("chute, #sos", True), ("#SOS", True),
    ("sosie du coureur", False), ("pas de SOS ici", False), ("#sosie", False), ("RAS", False),
    ("rejoignez #sos-secours38", False), ("canal #SOS38", False),
])
def test_sos_detection(content, expected):
    assert is_sos(content, ["SOS"]) is expected


def test_custom_sos_keywords():
    assert is_sos("Urgence au km 3", ["SOS", "urgence"])
    assert not is_sos("Urgence au km 3", ["SOS"])


@pytest.mark.parametrize("content, expected", [
    ("je suis à 45.123, 5.456", (45.123, 5.456)),
    ("45.123 5.456", (45.123, 5.456)),
    ("POS 45.1,5.7", (45.1, 5.7)),
    ("gps: -12.5 -77.25", (-12.5, -77.25)),
    ("ici 45.12345, 5.45678.", (45.12345, 5.45678)),
    ("km 18.5, 20.3", None),          # moins de 3 décimales sans préfixe
    ("95.123, 5.456", None),          # latitude hors limites
    ("45.123, 190.456", None),        # longitude hors limites
    ("version 1.2.345", None),
    ("RAS", None),
])
def test_position_detection(content, expected):
    assert find_position(content) == expected


def test_refused_by_default():
    assert event_log_message(msg("Ravito km 18: RAS"), EventLogConfig()) is None
    assert AgentConfig.model_validate({"identity": {"org": "o", "site": "s", "gateway": "g"}}).event_log.channels == []


def test_allowed_by_name_case_and_spaces():
    cfg = EventLogConfig(channels=["  organisation "])
    assert event_log_message(msg("a: b"), cfg) is not None
    assert event_log_message(msg("a: b", idx=0, name="Public"), cfg) is None


def test_allowed_by_index():
    cfg = EventLogConfig(channels=["#1"])
    p = event_log_message(msg("a: b", name=None), cfg)
    assert p is not None and p["channel"] == "#1"
    assert event_log_message(msg("a: b", idx=2, name=None), cfg) is None
    # un nom ne vaut pas index, et inversement
    assert event_log_message(msg("a: b", idx=1, name=None), EventLogConfig(channels=["Organisation"])) is None


def test_message_kinds_and_fields():
    m = event_log_message(msg("Ravito km 18: Besoin de 2 packs d'eau"), ORGA)
    assert m["kind"] == "message" and m["from_name"] == "Ravito km 18" and m["from_key"] is None
    assert m["channel"] == "Organisation" and m["lat"] is None and m["content"] == "Besoin de 2 packs d'eau"
    p = event_log_message(msg("Serre-file: POS 45.2,5.7"), ORGA)
    assert p["kind"] == "position" and (p["lat"], p["lng"]) == (45.2, 5.7)
    s = event_log_message(msg("PC Course: SOS coureur blessé 45.2051, 5.7398"), ORGA)
    assert s["kind"] == "sos" and (s["lat"], s["lng"]) == (45.2051, 5.7398)
    EventLog.model_validate(s)


def test_content_truncated_and_empty_dropped():
    m = event_log_message(msg("A: " + "é" * 800), ORGA)
    assert len(m["content"]) == 500
    assert event_log_message(msg("A:   "), ORGA) is None


@pytest.mark.parametrize("sender_ts, expected", [
    (NOW - timedelta(minutes=3), NOW - timedelta(minutes=3)),
    (NOW + timedelta(minutes=9), NOW + timedelta(minutes=9)),
    (NOW + timedelta(minutes=11), NOW),     # horloge de l'émetteur trop en avance
    (NOW - timedelta(minutes=11), NOW),     # horloge de l'émetteur en retard : heure de réception
    (NOW - timedelta(hours=25), NOW),       # horloge arrêtée la veille : jamais rattaché à l'événement passé
    (None, NOW),
])
def test_timestamp_rule(sender_ts, expected):
    m = event_log_message(msg("a: b", sender_ts=sender_ts), ORGA)
    assert m["ts"] == expected.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_payload_mapping_from_meshcore():
    p = {"type": "CHAN", "channel_idx": 1, "path_len": 2, "txt_type": 0, "sender_timestamp": int(NOW.timestamp()),
         "text": "Ravito: RAS", "SNR": 6.25}
    m = channel_payload_to_message(1, "Organisation", p, NOW)
    assert m.sender_ts == NOW and m.snr == 6.25 and m.text == "Ravito: RAS"
    assert channel_payload_to_message(1, None, {**p, "sender_timestamp": 0}, NOW).sender_ts is None
    assert channel_payload_to_message(1, None, {"channel_idx": 1}, NOW) is None


def test_collector_publishes_only_allowed_channels(cfg):
    cfg = cfg.model_copy(update={"event_log": ORGA})
    box = Outbox(":memory:")
    col = PassiveCollector(cfg, box, lambda: None)
    col.handle(msg("Rando38: Salut", idx=0, name="Public"))
    col.handle(msg("Ravito km 18: SOS 45.205, 5.740"))
    [m] = box.peek()
    assert m.topic == "mv/org-1/site-1/gw-1/event_log" and m.qos == 1
    assert m.payload["kind"] == "sos" and m.payload["from_name"] == "Ravito km 18"


def test_simulator_channels_are_filtered(cfg):
    cfg = cfg.model_copy(update={"event_log": ORGA})
    radio = SimulatedRadio(rng=random.Random(5))
    events = [radio.next_event() for _ in range(3000)]
    chans = [e for e in events if isinstance(e, ChannelMessage)]
    assert {e.channel_idx for e in chans} == {0, 1}
    assert any(e.text.split(": ", 1)[1].startswith("SOS") for e in chans if e.channel_idx == 1)
    box = Outbox(":memory:")
    col = PassiveCollector(cfg, box, lambda: None)
    for e in chans:
        col.handle(e)
    out = box.peek(limit=10_000)
    assert out and len(out) == sum(e.channel_idx == 1 for e in chans)
    assert all(m.payload["channel"] == "Organisation" for m in out)
    assert any(m.payload["kind"] == "sos" and m.payload["lat"] is not None for m in out)


class _FakeCommands:
    def __init__(self, fail: bool) -> None:
        self.fail = fail
        self.calls = 0

    async def get_channel(self, idx):
        from meshcore import EventType

        self.calls += 1
        if self.fail:
            raise TimeoutError
        return SimpleNamespace(type=EventType.CHANNEL_INFO,
                               payload={"channel_idx": idx, "channel_name": "Organisation",
                                        "channel_secret": b"\x01" * 16, "channel_hash": "ab"})


async def test_meshcore_channel_name_cached_and_secret_dropped(caplog):
    radio = MeshCoreRadio(RadioConfig())
    radio._mc = SimpleNamespace(commands=_FakeCommands(fail=False))
    ev = SimpleNamespace(payload={"channel_idx": 1, "text": "A: b", "sender_timestamp": 0})
    await radio._on_channel_msg(ev)
    await radio._on_channel_msg(ev)
    assert radio._mc.commands.calls == 1 and radio._channel_names == {1: "Organisation"}
    e = await asyncio.wait_for(radio.events(), 1)
    assert e.channel_name == "Organisation"
    assert "secret" not in repr(vars(radio)) and "\\x01" not in caplog.text


async def test_meshcore_get_channel_error_does_not_break_listening():
    radio = MeshCoreRadio(RadioConfig())
    radio._mc = SimpleNamespace(commands=_FakeCommands(fail=True))
    await radio._on_channel_msg(SimpleNamespace(payload={"channel_idx": 3, "text": "A: b"}))
    e = await asyncio.wait_for(radio.events(), 1)
    assert e.channel_idx == 3 and e.channel_name is None and radio._channel_names == {}


async def test_meshcore_never_listens_to_private_messages():
    from meshcore import EventType

    subscribed = []

    class FakeMC:
        self_info = {"public_key": "f0" * 32}
        contacts: dict = {}
        is_connected = True

        async def ensure_contacts(self, follow=False):
            return True

        def subscribe(self, event_type, cb):
            subscribed.append(event_type)

        async def start_auto_message_fetching(self):
            subscribed.append("auto_fetch")

    radio = MeshCoreRadio(RadioConfig())
    import meshcore

    orig = meshcore.MeshCore.create_serial

    async def fake_create(*a, **k):
        return FakeMC()

    meshcore.MeshCore.create_serial = fake_create
    try:
        await radio.connect()
    finally:
        meshcore.MeshCore.create_serial = orig
    assert EventType.CHANNEL_MSG_RECV in subscribed and "auto_fetch" in subscribed
    assert EventType.CONTACT_MSG_RECV not in subscribed
