# SPDX-License-Identifier: Apache-2.0
"""Tests d'intégration avec le broker Mosquitto local (infra/docker-compose.yml).

Lancer d'abord : infra/mosquitto/gen-dev.sh && docker compose -f infra/docker-compose.yml up -d
Ignorés automatiquement si le broker ou les identifiants de développement sont absents.
"""
import asyncio
import json
import random
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiomqtt
import pytest

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import PassiveCollector
from supervimesh_agent.commands import CommandHandler
from supervimesh_agent.config import AgentConfig
from supervimesh_agent.radio import AdvertSeen
from supervimesh_agent.uplink import Uplink

MOSQ = Path(__file__).resolve().parents[2] / "infra" / "mosquitto"
CREDS = MOSQ / "dev-credentials.env"
CA = MOSQ / "certs" / "ca.crt"


def _broker_up() -> bool:
    try:
        socket.create_connection(("localhost", 8883), timeout=1).close()
        return CREDS.exists()
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _broker_up(), reason="broker Mosquitto local non démarré")
CRED = dict(line.split("=", 1) for line in CREDS.read_text().split()) if CREDS.exists() else {}
GW1 = "mv/org-demo/site-demo/gw-1"
GW2 = "mv/org-autre/site-autre/gw-2"


def client(user: str, password: str | None = None, port: int = 8883, tls: bool = True) -> aiomqtt.Client:
    return aiomqtt.Client(
        "localhost", port, username=CRED.get(f"MQTT_{user}_USERNAME"),
        password=password or CRED.get(f"MQTT_{user}_PASSWORD"),
        tls_params=aiomqtt.TLSParameters(ca_certs=str(CA)) if tls else None, timeout=5,
    )


async def collect(sub: aiomqtt.Client, seconds: float = 1.5) -> list[aiomqtt.Message]:
    got: list[aiomqtt.Message] = []

    async def reader() -> None:
        async for m in sub.messages:
            got.append(m)

    try:
        await asyncio.wait_for(reader(), seconds)
    except TimeoutError:
        pass
    return got


async def test_plaintext_port_closed():
    with pytest.raises(aiomqtt.MqttError):
        async with client("GW1", port=1883, tls=False):
            pass


async def test_wrong_password_refused():
    with pytest.raises(aiomqtt.MqttError):
        async with client("GW1", password="mauvais-mot-de-passe"):
            pass


async def test_gateway_isolation():
    async with client("INGEST") as ingest, client("GW1") as gw1, client("GW2") as gw2:
        await ingest.subscribe("mv/#", qos=1)
        await gw2.subscribe("mv/#", qos=1)  # tente de tout lire : ne doit recevoir que son propre cmd
        await gw1.publish(f"{GW1}/node/advert", b'{"ok":1}', qos=1)          # autorisé
        await gw1.publish(f"{GW2}/node/advert", b'{"usurpation":1}', qos=1)  # sous-arbre d'une autre passerelle
        await gw1.publish(f"{GW1}/cmd", b'{"auto-commande":1}', qos=1)       # cmd interdit en écriture
        await gw1.publish("mv/org-demo/site-demo/gw-1/autre", b"{}", qos=1)   # sujet hors liste
        await ingest.publish(f"{GW1}/cmd", b'{"pour":"gw-1"}', qos=1)         # cmd d'une autre passerelle
        ingest_got = [str(m.topic) for m in await collect(ingest)]
        gw2_got = await collect(gw2, 0.5)
    assert f"{GW1}/node/advert" in ingest_got
    assert f"{GW2}/node/advert" not in ingest_got
    assert f"{GW1}/autre" not in ingest_got
    assert ingest_got.count(f"{GW1}/cmd") == 1  # seulement celui publié par l'ingestion
    assert gw2_got == []


def agent_cfg(tmp_path: Path) -> AgentConfig:
    return AgentConfig.model_validate({
        "identity": {"org": "org-demo", "site": "site-demo", "gateway": "gw-1"},
        "uplink": {"host": "localhost", "username": CRED.get("MQTT_GW1_USERNAME"),
                   "password": CRED.get("MQTT_GW1_PASSWORD"), "ca_file": str(CA), "heartbeat_interval_s": 10},
        "radio": {"transport": "simulator"},
        "buffer_path": str(tmp_path / "buffer.db"),
    })


async def _noop() -> None:
    pass


async def test_agent_end_to_end(tmp_path):
    cfg = agent_cfg(tmp_path)
    outbox = Outbox(cfg.buffer_path)
    # Message mis en tampon AVANT la connexion (coupure internet) : il doit partir avec son ts d'origine.
    old = AdvertSeen("a1b2c3d4e5f6", "Relais", "repeater", 45.1, 5.7, 1, ts=datetime(2026, 10, 28, 8, 0, tzinfo=timezone.utc))
    PassiveCollector(cfg, outbox, lambda: None).handle(old)
    uplink = Uplink(cfg, outbox, lambda: True, CommandHandler(cfg, _noop))

    async with client("INGEST") as ingest:
        await ingest.subscribe("mv/#", qos=1)
        task = asyncio.create_task(uplink.run())
        await asyncio.sleep(2)
        now = datetime.now(timezone.utc)
        for rid, exp in (("r-ping", now + timedelta(minutes=5)), ("r-old", now - timedelta(minutes=1))):
            body = {"schema_version": 1, "request_id": rid, "cmd": "ping", "expires_at": exp.strftime("%Y-%m-%dT%H:%M:%SZ")}
            await ingest.publish(f"{GW1}/cmd", json.dumps(body).encode(), qos=1)
        got = await collect(ingest, 7)
        task.cancel()

    by_topic: dict[str, list[dict]] = {}
    for m in got:
        by_topic.setdefault(str(m.topic).removeprefix(GW1 + "/"), []).append(json.loads(m.payload))
    # Un « offline » retenu d'un test précédent peut arriver d'abord : seul le dernier état compte.
    assert by_topic["status"][-1]["state"] == "online"
    assert by_topic["heartbeat"][0]["radio_connected"] is True
    assert by_topic["node/advert"][0]["ts"] == "2026-10-28T08:00:00Z"
    results = {r["request_id"]: r for r in by_topic["cmd/result"]}
    assert results["r-ping"]["ok"] is True
    assert results["r-old"] == {**results["r-old"], "ok": False, "error": "expired"}
    assert outbox.pending_count() == 0


async def test_last_will_marks_gateway_offline(tmp_path):
    """Coupure brutale de la passerelle : le broker publie le testament « offline » (retenu)."""
    cfg = agent_cfg(tmp_path)
    uplink = Uplink(cfg, Outbox(cfg.buffer_path), lambda: True, CommandHandler(cfg, _noop))
    c = uplink._client()
    await c.__aenter__()
    await c.publish(f"{GW1}/status", b'{"schema_version":1,"state":"online"}', qos=1, retain=True)
    c._client._sock.close()  # simule une coupure réseau (pas de DISCONNECT propre)
    await asyncio.sleep(random.uniform(1.5, 2))
    async with client("INGEST") as ingest:
        await ingest.subscribe(f"{GW1}/status", qos=1)
        got = await collect(ingest, 90)
    states = [json.loads(m.payload)["state"] for m in got]
    assert states and states[-1] == "offline"
