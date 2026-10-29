# SPDX-License-Identifier: Apache-2.0
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import PassiveCollector, StatusPoller
from supervimesh_agent.config import AgentConfig, load_config
from supervimesh_agent.radio import (
    AdvertSeen, PacketHeard, contact_to_advert, status_payload_to_report,
)
from supervimesh_agent.simulator import SimulatedRadio

KEY = "a1b2c3d4e5f6" + "0" * 52  # clé publique MeshCore fictive, pas un secret  gitleaks:allow


def test_example_config_is_valid():
    cfg = load_config(Path(__file__).parent.parent / "config.example.yaml")
    assert cfg.identity.gateway == "gw-1"


def test_password_never_in_repr(cfg):
    assert "secret" not in repr(cfg)


def test_polling_budget_cannot_flood_network():
    with pytest.raises(ValidationError):
        AgentConfig.model_validate({
            "identity": {"org": "o", "site": "s", "gateway": "g"},
            "polling": {"status_min_interval_min": 1},
        })


def test_outbox_survives_restart_and_dedupes(tmp_path):
    path = tmp_path / "b.db"
    box = Outbox(path)
    payload = {"msg_id": "m1", "ts": "2026-10-28T10:00:00Z"}
    assert box.enqueue("t", payload)
    assert not box.enqueue("t", payload)  # doublon ignoré
    box.close()
    box = Outbox(path)  # redémarrage : le message est toujours là, avec son ts d'origine
    [msg] = box.peek()
    assert msg.payload["ts"] == "2026-10-28T10:00:00Z"
    box.ack([msg.row_id])
    assert box.pending_count() == 0


def test_contact_mapping_hides_null_island():
    adv = contact_to_advert(KEY, {"adv_name": "Relais", "type": 2, "adv_lat": 0.0, "adv_lon": 0.0})
    assert adv.role == "repeater" and adv.lat is None and adv.lng is None


def test_status_mapping_never_invents_fields():
    r = status_payload_to_report(KEY, {"bat": 3920, "uptime": 100, "pubkey_pre": "a1b2"})
    assert r.battery_mv == 3920 and r.noise_floor_dbm is None and r.airtime_tx_s is None


def test_passive_collector_publishes_advert_and_aggregated_links(cfg):
    box = Outbox(":memory:")
    col = PassiveCollector(cfg, box, lambda: "f0" * 32)
    col.handle(PacketHeard(snr=6.0, rssi=-100, last_hop="a1"))
    col.handle(PacketHeard(snr=8.0, rssi=-102, last_hop="a1"))
    col.handle(AdvertSeen(KEY, "Relais", "repeater", 45.1, 5.7, hops=0))
    assert col.flush_links(datetime.now(timezone.utc)) == 1
    advert, link = box.peek()
    assert advert.topic == "mv/org-1/site-1/gw-1/node/advert"
    assert advert.payload["schema_version"] == 1 and advert.payload["snr"] == 8.0
    assert link.topic.endswith("/link") and link.qos == 0
    assert link.payload["snr"] == 7.0 and link.payload["count"] == 2


async def test_status_poller_with_simulator(cfg):
    box = Outbox(":memory:")
    radio = SimulatedRadio(rng=random.Random(1))
    await radio.connect()
    poller = StatusPoller(cfg, box, radio, rng=random.Random(1))
    assert await poller.poll_once(cfg.managed_repeaters[0])
    [msg] = box.peek()
    assert msg.topic.endswith("/node/status") and msg.payload["battery_mv"] <= 4000
    assert "password" not in str(msg.payload) and "secret" not in str(msg.payload)


async def test_silent_repeater_produces_no_status(cfg):
    box = Outbox(":memory:")
    radio = SimulatedRadio()
    await radio.connect()
    radio.nodes[KEY].alive = False
    assert not await StatusPoller(cfg, box, radio).poll_once(cfg.managed_repeaters[0])
    assert box.pending_count() == 0


def test_poll_delay_respects_budget(cfg):
    poller = StatusPoller(cfg, Outbox(":memory:"), SimulatedRadio(), rng=random.Random(0))
    delays = [poller.next_delay_s() for _ in range(200)]
    assert min(delays) >= 30 * 60 and max(delays) <= 60 * 60
    assert len(set(delays)) > 100  # gigue aléatoire


def test_simulator_emits_events():
    radio = SimulatedRadio(rng=random.Random(2))
    events = [radio.next_event() for _ in range(50)]
    assert any(isinstance(e, AdvertSeen) for e in events)
    assert any(isinstance(e, PacketHeard) for e in events)


def test_advert_signal_never_borrowed_from_another_node(cfg):
    box = Outbox(":memory:")
    col = PassiveCollector(cfg, box, lambda: None)
    t0 = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
    other = "b2" + KEY[2:]
    col.handle(PacketHeard(snr=5.0, rssi=-90, last_hop="b2", ts=t0))           # relayé par un autre nœud
    col.handle(AdvertSeen(KEY, "R", "repeater", None, None, 1, ts=t0))
    col.handle(PacketHeard(snr=5.0, rssi=-90, last_hop=None, ts=t0))
    col.handle(AdvertSeen(KEY, "R", "repeater", None, None, 0, ts=t0 + timedelta(seconds=5)))  # trop tard
    col.handle(PacketHeard(snr=4.0, rssi=-95, last_hop=None, ts=t0))
    col.handle(AdvertSeen(other, "B", "repeater", None, None, 0, ts=t0))       # direct, juste après : OK
    col.handle(AdvertSeen(KEY, "R", "repeater", None, None, 0, ts=t0))        # paquet déjà consommé
    signals = [(m.payload["rssi"], m.payload["snr"]) for m in box.peek()]
    assert signals == [(None, None), (None, None), (-95, 4.0), (None, None)]
