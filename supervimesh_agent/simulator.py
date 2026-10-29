# SPDX-License-Identifier: Apache-2.0
"""Simulateur radio : faux réseau MeshCore pour développer et tester sans matériel."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

from .config import ManagedRepeater
from .radio import AdvertSeen, PacketHeard, RadioEvent, StatusReport, TestOutcome


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
]


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

    async def connect(self) -> None:
        self._connected = True

    async def close(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def set_connected(self, value: bool) -> None:
        """Simule un débranchement / rebranchement du boîtier."""
        self._connected = value

    def next_event(self) -> RadioEvent:
        alive = [n for n in self.nodes.values() if n.alive]
        node = self._rng.choice(alive)
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

    async def run_test(self, target_key: str, timeout_s: float) -> TestOutcome:
        node = next((n for k, n in self.nodes.items() if k.startswith(target_key)), None)
        if node is None:
            return TestOutcome(False, None, [], "nœud cible inconnu du companion")
        if not node.alive or not self._connected:
            return TestOutcome(False, None, [], "pas de réponse du nœud cible")
        hops = [n.public_key[:2] for n in self.nodes.values() if n.role == "repeater" and n.alive][: self._rng.randint(0, 2)]
        return TestOutcome(True, self._rng.randint(800, 3500), [*hops, node.public_key[:2]], None)
