# SPDX-License-Identifier: Apache-2.0
"""Transforme les événements radio en messages sortants (spécification MQTT, docs/mqtt-spec.md)."""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from .buffer import Outbox
from .config import AgentConfig, ManagedRepeater
from .radio import AdvertSeen, PacketHeard, Radio, RadioEvent, StatusReport

log = logging.getLogger(__name__)
SCHEMA_VERSION = 1
MAX_ADVERT_SIGNAL_DELAY_S = 1.0


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def envelope(ts: datetime, **fields: Any) -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "msg_id": str(uuid.uuid4()), "ts": iso(ts), **fields}


class Topics:
    def __init__(self, cfg: AgentConfig) -> None:
        i = cfg.identity
        self.base = f"mv/{i.org}/{i.site}/{i.gateway}"

    def __call__(self, suffix: str) -> str:
        return f"{self.base}/{suffix}"


def advert_message(e: AdvertSeen, rssi: float | None = None, snr: float | None = None) -> dict[str, Any]:
    return envelope(
        e.ts, public_key=e.public_key, name=e.name, role=e.role,
        lat=e.lat, lng=e.lng, rssi=rssi, snr=snr, hops=e.hops,
    )


def status_message(r: StatusReport) -> dict[str, Any]:
    return envelope(
        r.ts, public_key=r.public_key, battery_mv=r.battery_mv, uptime_s=r.uptime_s,
        noise_floor_dbm=r.noise_floor_dbm, airtime_tx_s=r.airtime_tx_s,
        packets_rx=r.packets_rx, packets_tx=r.packets_tx, extra=r.extra,
    )


@dataclass
class _LinkAgg:
    snr_sum: float = 0.0
    snr_n: int = 0
    rssi_sum: float = 0.0
    rssi_n: int = 0
    count: int = 0

    def add(self, p: PacketHeard) -> None:
        self.count += 1
        if p.snr is not None:
            self.snr_sum, self.snr_n = self.snr_sum + p.snr, self.snr_n + 1
        if p.rssi is not None:
            self.rssi_sum, self.rssi_n = self.rssi_sum + p.rssi, self.rssi_n + 1


class PassiveCollector:
    """Écoute passive (aucune émission radio) : annonces et qualité des liaisons agrégée par minute."""

    def __init__(self, cfg: AgentConfig, outbox: Outbox, self_key: Callable[[], str | None]) -> None:
        self._topic = Topics(cfg)
        self._outbox = outbox
        self._self_key = self_key
        self._links: dict[str, _LinkAgg] = {}
        self._last_heard: PacketHeard | None = None

    def _signal_of(self, advert: AdvertSeen) -> PacketHeard | None:
        """Signal du paquet qui porte cette annonce, seulement si on peut l'établir.

        meshcore_py émet le journal du paquet juste avant l'annonce. On ne l'attribue au nœud que si le
        paquet vient d'arriver ET a été reçu en direct de ce nœud (pas relayé par un autre) ; sinon None.
        """
        last, self._last_heard = self._last_heard, None  # un paquet ne sert qu'à une annonce
        if last is None or not 0 <= (advert.ts - last.ts).total_seconds() <= MAX_ADVERT_SIGNAL_DELAY_S:
            return None
        if last.last_hop and not advert.public_key.startswith(last.last_hop):
            return None
        return last

    def handle(self, event: RadioEvent) -> None:
        if isinstance(event, AdvertSeen):
            sig = self._signal_of(event)
            self._outbox.enqueue(
                self._topic("node/advert"),
                advert_message(event, sig.rssi if sig else None, sig.snr if sig else None),
            )
        elif isinstance(event, PacketHeard):
            self._last_heard = event
            if event.last_hop:
                self._links.setdefault(event.last_hop, _LinkAgg()).add(event)

    def flush_links(self, now: datetime) -> int:
        """Publie une observation agrégée par liaison (dernier relais → passerelle)."""
        to_key = self._self_key()
        flushed = 0
        for hop, agg in self._links.items():
            self._outbox.enqueue(
                self._topic("link"),
                envelope(
                    now, **{"from": hop, "to": to_key},
                    snr=round(agg.snr_sum / agg.snr_n, 1) if agg.snr_n else None,
                    rssi=round(agg.rssi_sum / agg.rssi_n) if agg.rssi_n else None,
                    count=agg.count,
                ),
                qos=0,
            )
            flushed += 1
        self._links.clear()
        return flushed


class StatusPoller:
    """Interrogation active des relais administrés, avec budget radio et gigue aléatoire."""

    def __init__(
        self, cfg: AgentConfig, outbox: Outbox, radio: Radio, rng: random.Random | None = None
    ) -> None:
        self._cfg = cfg
        self._topic = Topics(cfg)
        self._outbox = outbox
        self._radio = radio
        self._rng = rng or random.Random()

    def next_delay_s(self) -> float:
        p = self._cfg.polling
        return self._rng.uniform(p.status_min_interval_min * 60, p.status_max_interval_min * 60)

    async def poll_once(self, repeater: ManagedRepeater) -> bool:
        try:
            report = await self._radio.request_status(repeater, self._cfg.polling.request_timeout_s)
        except Exception:  # la radio ne doit jamais faire tomber l'agent
            log.exception("échec de la demande de statut à %s", repeater.public_key[:12])
            return False
        if report is None:
            return False
        self._outbox.enqueue(self._topic("node/status"), status_message(report))
        return True

    async def run_for(self, repeater: ManagedRepeater) -> None:
        # Premier passage décalé aléatoirement pour éviter que tous les relais soient interrogés ensemble.
        await asyncio.sleep(self._rng.uniform(30, 300))
        while True:
            if self._radio.is_connected():
                await self.poll_once(repeater)
            await asyncio.sleep(self.next_delay_s())
