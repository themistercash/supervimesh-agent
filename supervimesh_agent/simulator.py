# SPDX-License-Identifier: Apache-2.0
"""Simulateur radio : faux réseau MeshCore pour développer et tester sans matériel."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .config import ManagedRepeater, Sensor
from .radio import AdvertSeen, ChannelMessage, PacketHeard, RadioEvent, StatusReport, TestOutcome, utcnow


@dataclass
class SimNode:
    public_key: str
    name: str
    role: str
    lat: float
    lng: float
    battery_mv: int = 4000
    alive: bool = True


DEFAULT_NODES = [
    SimNode("a1b2c3d4e5f6" + "0" * 52, "Relais Église", "repeater", 45.190, 5.720),
    SimNode("b2c3d4e5f6a1" + "0" * 52, "Relais Col", "repeater", 45.215, 5.760),
    SimNode("c3d4e5f6a1b2" + "0" * 52, "Ravito km 18", "companion", 45.205, 5.740),
    SimNode("d4e5f6a1b2c3" + "0" * 52, "Capteur Col", "sensor", 45.214, 5.758),
]


SIM_CHANNELS = {0: "Public", 1: "Organisation"}
ORGA_SENDERS = ["Ravito km 18", "PC Course", "Serre-file", "Signaleur col"]
ORGA_TEXTS = [
    "Besoin de 2 packs d'eau", "Tête de course passée", "RAS au poste", "Dernier coureur passé",
    "Ravito ouvert", "Position 45.2051, 5.7398",
]
PUBLIC_SENDERS = ["Rando38", "Spectateur", "Cycliste"]
PUBLIC_TEXTS = ["Salut à tous", "Quelqu'un m'entend ?", "Belle journée sur le col"]


class SimulatedRadio:
    def __init__(
        self,
        nodes: list[SimNode] | None = None,
        interval_s: float = 5.0,
        rng: random.Random | None = None,
    ) -> None:
        self.nodes = {n.public_key: n for n in (nodes or [SimNode(**vars(n)) for n in DEFAULT_NODES])}
        self.self_key: str | None = "f0" * 32
        self._interval = interval_s
        self._rng = rng or random.Random()
        self._connected = False
        self.status_requests = 0
        self.telemetry_requests = 0
        self.sent_messages: list[tuple[str, str]] = []

    async def connect(self) -> None:
        self._connected = True

    async def close(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def set_connected(self, value: bool) -> None:
        """Simule un débranchement / rebranchement du boîtier."""
        self._connected = value

    def _channel_message(self) -> ChannelMessage:
        """Canal « Organisation » (#1) en majorité, rarement un SOS avec position ; parfois « Public » (#0)."""
        now = utcnow()
        if self._rng.random() < 0.6:
            idx, sender = 1, self._rng.choice(ORGA_SENDERS)
            if self._rng.random() < 0.08:
                lat, lng = 45.205 + self._rng.uniform(-0.01, 0.01), 5.740 + self._rng.uniform(-0.01, 0.01)
                text = f"SOS coureur au sol {lat:.4f}, {lng:.4f}"
            else:
                text = self._rng.choice(ORGA_TEXTS)
        else:
            idx, sender, text = 0, self._rng.choice(PUBLIC_SENDERS), self._rng.choice(PUBLIC_TEXTS)
        return ChannelMessage(
            channel_idx=idx, channel_name=SIM_CHANNELS[idx], text=f"{sender}: {text}",
            sender_ts=now - timedelta(seconds=self._rng.randint(0, 20)), received_at=now,
            snr=round(self._rng.uniform(-5, 12), 1),
        )

    def next_event(self) -> RadioEvent:
        alive = [n for n in self.nodes.values() if n.alive]
        node = self._rng.choice(alive)
        if self._rng.random() < 0.06:
            return self._channel_message()
        if self._rng.random() < 0.3:
            return AdvertSeen(node.public_key, node.name, node.role, node.lat, node.lng, hops=self._rng.randint(0, 2))
        return PacketHeard(
            snr=round(self._rng.uniform(-5, 12), 1),
            rssi=self._rng.randint(-118, -70),
            last_hop=node.public_key[:2],
        )

    async def events(self) -> RadioEvent:
        while not self._connected or not any(n.alive for n in self.nodes.values()):
            await asyncio.sleep(self._interval)
        await asyncio.sleep(self._rng.expovariate(1 / self._interval))
        return self.next_event()

    async def request_status(self, repeater: ManagedRepeater, timeout_s: float) -> StatusReport | None:
        self.status_requests += 1
        node = self.nodes.get(repeater.public_key)
        if node is None or not node.alive or not self._connected:
            return None
        node.battery_mv = max(3300, node.battery_mv - self._rng.randint(0, 5))
        return StatusReport(
            public_key=node.public_key,
            battery_mv=node.battery_mv,
            uptime_s=self._rng.randint(3600, 2_000_000),
            noise_floor_dbm=self._rng.randint(-118, -105),
            airtime_tx_s=self._rng.randint(10, 500),
            packets_rx=self._rng.randint(1000, 20000),
            packets_tx=self._rng.randint(500, 8000),
        )

    async def request_telemetry(self, sensor: Sensor, timeout_s: float) -> list[dict[str, Any]] | None:
        """Mesures plausibles (format meshcore_py) : tension sur le canal 1, température et humidité sur le 2."""
        self.telemetry_requests += 1
        prefix = sensor.public_key.lower()
        node = next((n for k, n in self.nodes.items() if k.startswith(prefix)), None)
        if node is None or not node.alive or not self._connected:
            return None
        node.battery_mv = max(3300, node.battery_mv - self._rng.randint(0, 5))
        return [
            {"channel": 1, "type": "voltage", "value": round(node.battery_mv / 1000, 2)},
            {"channel": 2, "type": "temperature", "value": round(self._rng.uniform(-2.0, 18.0), 1)},
            {"channel": 2, "type": "humidity", "value": round(self._rng.uniform(45, 95), 1)},
        ]

    async def send_channel_message(self, channel: str, text: str) -> str | None:
        if not self._connected:
            return "radio déconnectée"
        if channel.strip().casefold() not in {n.casefold() for n in SIM_CHANNELS.values()}:
            return "canal absent du companion"
        self.sent_messages.append((channel, text))
        return None

    async def run_test(self, target_key: str, timeout_s: float) -> TestOutcome:
        node = next((n for k, n in self.nodes.items() if k.startswith(target_key)), None)
        if node is None:
            return TestOutcome(False, None, [], "nœud cible inconnu du companion")
        if not node.alive or not self._connected:
            return TestOutcome(False, None, [], "pas de réponse du nœud cible")
        hops = [n.public_key[:2] for n in self.nodes.values() if n.role == "repeater" and n.alive][: self._rng.randint(0, 2)]
        return TestOutcome(True, self._rng.randint(800, 3500), [*hops, node.public_key[:2]], None)
