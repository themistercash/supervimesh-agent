# SPDX-License-Identifier: Apache-2.0
"""Transforme les événements radio en messages sortants (spécification MQTT, docs/mqtt-spec.md)."""
from __future__ import annotations

import asyncio
import logging
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from pydantic import ValidationError

from .buffer import Outbox
from .config import AgentConfig, EventLogConfig, ManagedRepeater, Sensor
from .radio import (
    AdvertSeen, ChannelMessage, PacketHeard, Radio, RadioEvent, StatusReport, normalize_readings,
)
from .schemas import validate_outgoing

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


def telemetry_message(public_key: str, raw_readings: Any, ts: datetime) -> dict[str, Any] | None:
    """Relevé d'un capteur → node_telemetry, ou None s'il ne reste aucune mesure valable (rien à publier)."""
    readings = normalize_readings(raw_readings)
    if not readings:
        return None
    payload = envelope(ts, public_key=public_key.lower(), readings=readings)
    try:
        validate_outgoing("node/telemetry", payload)
    except ValidationError:
        log.warning("télémétrie du capteur %s hors contrat, ignorée", public_key[:12])
        return None
    return payload


MAX_CONTENT = 500
MAX_NAME = 64
MAX_FUTURE = timedelta(minutes=10)  # horloge de l'émetteur en avance : tolérée jusqu'à 10 min
# Heure de l'émetteur retenue seulement si elle est proche de la réception par la passerelle (horloge NTP) :
# un boîtier dont l'horloge est fausse ne doit pas faire entrer un message dans la fenêtre d'un événement passé.
MAX_PAST = timedelta(minutes=10)
_HASHTAG_SOS = re.compile(r"(?<!\w)#sos(?=$|\s|[!.,;:?])", re.IGNORECASE)  # pas « #sos-38 »
# Coordonnées décimales « lat, lng » ou « lat lng ». Sans préfixe : au moins 3 décimales de chaque côté
# (évite « km 18.5 »). Avec le préfixe POS / GPS / position : au moins 1 décimale.
_NUM = r"[-+]?\d{1,3}\.\d{%s,}"
_COORDS = re.compile(
    r"(?<![\w.])(?:(?:pos|gps|position)\s*:?\s*(?P<plat>%s)\s*(?:[,;]\s*|\s+)(?P<plng>%s)"
    r"|(?P<lat>%s)\s*(?:[,;]\s*|\s+)(?P<lng>%s))(?!\w|\.\d)" % (_NUM % 1, _NUM % 1, _NUM % 3, _NUM % 3),
    re.IGNORECASE,
)


def split_sender(text: str) -> tuple[str | None, str]:
    """Convention MeshCore « Nom: message » → (nom, message). Sans préfixe valable : (None, texte)."""
    name, sep, rest = text.partition(": ")
    name = name.strip()
    if sep and name and len(name) <= MAX_NAME and "\n" not in name:
        return name, rest.strip()
    return None, text.strip()


def is_sos(content: str, keywords: list[str]) -> bool:
    """Commence par un mot-clé (mot entier, casse ignorée) ou contient « #sos »."""
    for kw in keywords:
        kw = kw.strip()
        if kw and re.match(r"\s*%s(?!\w)" % re.escape(kw), content, re.IGNORECASE):
            return True
    return bool(_HASHTAG_SOS.search(content))


def find_position(content: str) -> tuple[float, float] | None:
    """Premières coordonnées décimales plausibles (lat ∈ [-90, 90], lng ∈ [-180, 180])."""
    for m in _COORDS.finditer(content):
        lat, lng = float(m["plat"] or m["lat"]), float(m["plng"] or m["lng"])
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            return lat, lng
    return None


def event_ts(msg: ChannelMessage) -> datetime:
    """Heure de l'émetteur si elle est plausible, sinon heure de réception."""
    s = msg.sender_ts
    if s is not None and msg.received_at - MAX_PAST <= s <= msg.received_at + MAX_FUTURE:
        return s
    return msg.received_at


def event_log_message(msg: ChannelMessage, cfg: EventLogConfig) -> dict[str, Any] | None:
    """Message de canal → event_log, ou None si le canal n'est pas autorisé (refus par défaut)."""
    if not cfg.allows(msg.channel_idx, msg.channel_name):
        return None
    from_name, content = split_sender(msg.text)
    if not content:
        return None
    content = content[:MAX_CONTENT]
    pos = find_position(content)
    kind = "sos" if is_sos(content, cfg.sos_keywords) else "position" if pos else "message"
    payload = envelope(
        event_ts(msg), channel=(msg.channel_name or f"#{msg.channel_idx}")[:MAX_NAME],
        from_name=from_name, from_key=None, kind=kind, content=content,
        lat=pos[0] if pos else None, lng=pos[1] if pos else None,
    )
    try:
        validate_outgoing("event_log", payload)
    except ValidationError:
        log.warning("message du canal #%d hors contrat, ignoré", msg.channel_idx)
        return None
    return payload


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
        self._event_log = cfg.event_log
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
        elif isinstance(event, ChannelMessage):
            payload = event_log_message(event, self._event_log)
            if payload is not None:
                self._outbox.enqueue(self._topic("event_log"), payload, qos=1)

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


class TelemetryPoller:
    """Relevé périodique de la télémétrie des capteurs, avec budget radio et gigue aléatoire."""

    JITTER = 0.2  # jusqu'à +20 % sur l'intervalle configuré, jamais en dessous

    def __init__(
        self, cfg: AgentConfig, outbox: Outbox, radio: Radio, rng: random.Random | None = None
    ) -> None:
        self._cfg = cfg
        self._topic = Topics(cfg)
        self._outbox = outbox
        self._radio = radio
        self._rng = rng or random.Random()

    def next_delay_s(self, sensor: Sensor) -> float:
        base = sensor.interval_min * 60
        return self._rng.uniform(base, base * (1 + self.JITTER))

    async def poll_once(self, sensor: Sensor) -> bool:
        try:
            raw = await self._radio.request_telemetry(sensor, self._cfg.polling.request_timeout_s)
        except Exception:  # la radio ne doit jamais faire tomber l'agent
            log.exception("échec du relevé du capteur %s", sensor.public_key[:12])
            return False
        if raw is None:
            return False
        payload = telemetry_message(sensor.public_key, raw, datetime.now(timezone.utc))
        if payload is None:
            log.info("capteur %s : aucune mesure exploitable", sensor.public_key[:12])
            return False
        self._outbox.enqueue(self._topic("node/telemetry"), payload, qos=1)
        return True

    async def run_for(self, sensor: Sensor) -> None:
        # Premier relevé décalé aléatoirement pour ne pas interroger tous les capteurs (et relais) ensemble.
        await asyncio.sleep(self._rng.uniform(30, 300))
        while True:
            if self._radio.is_connected():
                await self.poll_once(sensor)
            await asyncio.sleep(self.next_delay_s(sensor))
