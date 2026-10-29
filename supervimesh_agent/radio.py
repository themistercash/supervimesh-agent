# SPDX-License-Identifier: Apache-2.0
"""Accès radio : MeshCore réel ou simulateur, derrière une même interface.

Les événements bruts de meshcore_py sont traduits en événements normalisés. Les champs
indisponibles restent à None : on n'invente jamais une mesure.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from .config import ManagedRepeater, RadioConfig

log = logging.getLogger(__name__)

# Types d'annonce MeshCore (champ `type` des contacts).
ROLE_BY_TYPE = {1: "companion", 2: "repeater", 3: "room_server", 4: "sensor"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class AdvertSeen:
    public_key: str
    name: str | None
    role: str
    lat: float | None
    lng: float | None
    hops: int | None
    ts: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class PacketHeard:
    """Paquet entendu par le companion. `last_hop` = préfixe de hash du dernier relais du chemin."""

    snr: float | None
    rssi: int | None
    last_hop: str | None
    ts: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class StatusReport:
    public_key: str
    battery_mv: int | None
    uptime_s: int | None
    noise_floor_dbm: int | None
    airtime_tx_s: int | None
    packets_rx: int | None
    packets_tx: int | None
    extra: dict[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class TestOutcome:
    """Résultat d'un test réseau (aller-retour de découverte de chemin vers le nœud cible)."""

    delivered: bool
    latency_ms: int | None
    path: list[str]
    error: str | None
    ts: datetime = field(default_factory=utcnow)


RadioEvent = AdvertSeen | PacketHeard


class Radio(Protocol):
    self_key: str | None

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    def is_connected(self) -> bool: ...
    async def events(self) -> RadioEvent: ...
    async def request_status(self, repeater: ManagedRepeater, timeout_s: float) -> StatusReport | None: ...
    async def run_test(self, target_key: str, timeout_s: float) -> TestOutcome: ...


def _num(value: Any) -> Any:
    return value if isinstance(value, (int, float)) else None


def contact_to_advert(public_key: str, contact: dict[str, Any]) -> AdvertSeen:
    lat, lng = _num(contact.get("adv_lat")), _num(contact.get("adv_lon"))
    if lat == 0 and lng == 0:  # MeshCore envoie 0,0 quand la position n'est pas partagée
        lat = lng = None
    out_len = contact.get("out_path_len")
    return AdvertSeen(
        public_key=public_key,
        name=contact.get("adv_name") or None,
        role=ROLE_BY_TYPE.get(contact.get("type", 0), "unknown"),
        lat=lat,
        lng=lng,
        hops=out_len if isinstance(out_len, int) and out_len >= 0 else None,
    )


def status_payload_to_report(public_key: str, p: dict[str, Any]) -> StatusReport:
    known = {"bat", "uptime", "noise_floor", "airtime", "nb_recv", "nb_sent", "pubkey_pre"}
    return StatusReport(
        public_key=public_key,
        battery_mv=_num(p.get("bat")),
        uptime_s=_num(p.get("uptime")),
        noise_floor_dbm=_num(p.get("noise_floor")),
        airtime_tx_s=_num(p.get("airtime")),
        packets_rx=_num(p.get("nb_recv")),
        packets_tx=_num(p.get("nb_sent")),
        extra={k: v for k, v in p.items() if k not in known and isinstance(v, (int, float))},
    )


class MeshCoreRadio:
    """Boîtier companion réel via meshcore_py (USB série ou TCP)."""

    def __init__(self, cfg: RadioConfig) -> None:
        self._cfg = cfg
        self._mc: Any = None
        self._queue: asyncio.Queue[RadioEvent] = asyncio.Queue(maxsize=1000)
        self.self_key: str | None = None

    async def connect(self) -> None:
        from meshcore import EventType, MeshCore

        if self._cfg.transport == "tcp":
            mc = await MeshCore.create_tcp(self._cfg.host, self._cfg.tcp_port, auto_reconnect=True)
        else:
            mc = await MeshCore.create_serial(self._cfg.port, self._cfg.baudrate, auto_reconnect=True)
        if mc is None:
            raise ConnectionError("aucune réponse du boîtier companion")
        self._mc = mc
        self.self_key = (mc.self_info or {}).get("public_key")
        await mc.ensure_contacts()
        mc.subscribe(EventType.ADVERTISEMENT, self._on_advert)
        mc.subscribe(EventType.RX_LOG_DATA, self._on_rx)
        log.info("radio connectée (%d contacts connus)", len(mc.contacts))

    async def close(self) -> None:
        if self._mc is not None:
            await self._mc.disconnect()
            self._mc = None

    def is_connected(self) -> bool:
        return self._mc is not None and bool(self._mc.is_connected)

    async def events(self) -> RadioEvent:
        return await self._queue.get()

    def _put(self, event: RadioEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            log.warning("file radio pleine, événement ignoré")

    async def _on_advert(self, event: Any) -> None:
        key = event.payload["public_key"]
        await self._mc.ensure_contacts(follow=True)
        self._put(contact_to_advert(key, self._mc.contacts.get(key, {})))

    async def _on_rx(self, event: Any) -> None:
        p = event.payload
        path = event.attributes.get("path") or p.get("path") or ""
        path_len = event.attributes.get("path_len") or 0
        hop_len = len(path) // path_len if path_len else 0
        self._put(
            PacketHeard(
                snr=_num(p.get("snr")),
                rssi=_num(p.get("rssi")),
                last_hop=path[-hop_len:] if hop_len else None,
            )
        )

    async def request_status(self, repeater: ManagedRepeater, timeout_s: float) -> StatusReport | None:
        contact = self._mc.get_contact_by_key_prefix(repeater.public_key)
        if contact is None:
            log.warning("relais %s inconnu du companion", repeater.public_key[:12])
            return None
        login = await self._mc.commands.send_login_sync(contact, repeater.password, timeout=timeout_s)
        if login is None:  # seul LOGIN_SUCCESS est renvoyé ; None = refus ou délai dépassé
            log.warning("connexion admin refusée par %s", repeater.public_key[:12])
            return None
        payload = await self._mc.commands.req_status_sync(contact, timeout=timeout_s)
        if not payload:
            return None
        return status_payload_to_report(contact["public_key"], payload)

    async def run_test(self, target_key: str, timeout_s: float) -> TestOutcome:
        """Test réseau : découverte de chemin vers la cible (aller-retour, sans mot de passe administrateur)."""
        contact = self._mc.get_contact_by_key_prefix(target_key)
        if contact is None:
            return TestOutcome(False, None, [], "nœud cible inconnu du companion")
        started = time.monotonic()
        event = await self._mc.commands.send_path_discovery_sync(contact, timeout=timeout_s)
        latency = int((time.monotonic() - started) * 1000)
        if event is None:
            return TestOutcome(False, None, [], "pas de réponse du nœud cible")
        p = event.payload
        if not str(contact["public_key"]).startswith(str(p.get("pubkey_pre", ""))):
            return TestOutcome(False, None, [], "réponse reçue d'un autre nœud")
        return TestOutcome(True, latency, split_path(p.get("out_path", ""), p.get("out_path_hash_len", 1)), None)


def split_path(hex_path: str, hash_len: int) -> list[str]:
    """« a1b2c3 » avec des hachages de 1 octet → ["a1", "b2", "c3"]."""
    step = max(1, hash_len) * 2
    return [hex_path[i : i + step] for i in range(0, len(hex_path), step) if hex_path[i : i + step]]

