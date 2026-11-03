# SPDX-License-Identifier: Apache-2.0
"""État du réseau conservé sur la passerelle, pour le tableau de bord local (fonctionne sans internet).

Alimenté par chaque message mis dans le tampon : il reste donc à jour même quand le cloud est injoignable,
et il survit aux redémarrages (SQLite). Aucun mot de passe n'y est stocké.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)
SILENT_AFTER = timedelta(hours=2)  # même seuil que l'alerte « Relais muet » de la plateforme
BATTERY_LOW_MV = 3500  # même seuil que l'alerte « Batterie faible »
MAX_TESTS = 50
MAX_LOG = 200  # main courante : derniers messages gardés sur la passerelle
LOG_SHOWN = 30

_SCHEMA = """
CREATE TABLE IF NOT EXISTS local_nodes (
    public_key TEXT PRIMARY KEY,
    name TEXT, role TEXT, lat REAL, lng REAL, hops INTEGER, rssi REAL, snr REAL,
    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
    battery_mv INTEGER, uptime_s INTEGER, noise_floor_dbm REAL, status_at TEXT
);
CREATE TABLE IF NOT EXISTS local_links (
    hop TEXT PRIMARY KEY, ts TEXT NOT NULL, snr REAL, rssi REAL, count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS local_tests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, target TEXT NOT NULL, delivered INTEGER NOT NULL, latency_ms INTEGER, error TEXT
);
CREATE TABLE IF NOT EXISTS local_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, channel TEXT NOT NULL, from_name TEXT, kind TEXT NOT NULL, content TEXT, lat REAL, lng REAL
);
"""


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class LocalState:
    def __init__(self, path: Path | str) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False)  # accès séquentiels depuis la boucle asyncio
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)

    def observe(self, topic: str, payload: dict[str, Any]) -> None:
        """Appelé pour chaque message sortant ; une erreur ici ne doit jamais bloquer l'envoi."""
        suffix = topic.split("/", 4)[4] if topic.count("/") >= 4 else topic
        try:
            handler = {
                "node/advert": self._advert, "node/status": self._status,
                "link": self._link, "test_result": self._test, "event_log": self._log,
            }.get(suffix)
            if handler:
                handler(payload)
                self._db.commit()
        except (sqlite3.Error, KeyError, TypeError, ValueError):
            log.exception("état local : message %s ignoré", suffix)

    def _advert(self, p: dict[str, Any]) -> None:
        # Un message plus ancien (sorti du tampon après coupure) n'écrase pas une information plus récente.
        self._db.execute(
            """INSERT INTO local_nodes (public_key, name, role, lat, lng, hops, rssi, snr, first_seen, last_seen)
               VALUES (:k, :name, :role, :lat, :lng, :hops, :rssi, :snr, :ts, :ts)
               ON CONFLICT (public_key) DO UPDATE SET
                 name = CASE WHEN :ts >= last_seen THEN coalesce(:name, name) ELSE name END,
                 role = CASE WHEN :ts >= last_seen THEN coalesce(:role, role) ELSE role END,
                 lat = CASE WHEN :ts >= last_seen AND :lat IS NOT NULL THEN :lat ELSE lat END,
                 lng = CASE WHEN :ts >= last_seen AND :lng IS NOT NULL THEN :lng ELSE lng END,
                 hops = CASE WHEN :ts >= last_seen THEN :hops ELSE hops END,
                 rssi = CASE WHEN :ts >= last_seen AND :rssi IS NOT NULL THEN :rssi ELSE rssi END,
                 snr = CASE WHEN :ts >= last_seen AND :snr IS NOT NULL THEN :snr ELSE snr END,
                 last_seen = max(last_seen, :ts)""",
            {"k": p["public_key"].lower(), "name": p.get("name"), "role": p.get("role"), "lat": p.get("lat"),
             "lng": p.get("lng"), "hops": p.get("hops"), "rssi": p.get("rssi"), "snr": p.get("snr"), "ts": p["ts"]},
        )

    def _status(self, p: dict[str, Any]) -> None:
        # Une réponse de statut prouve aussi que le relais est vivant.
        self._db.execute(
            """INSERT INTO local_nodes (public_key, first_seen, last_seen, battery_mv, uptime_s, noise_floor_dbm, status_at)
               VALUES (:k, :ts, :ts, :bat, :up, :noise, :ts)
               ON CONFLICT (public_key) DO UPDATE SET
                 last_seen = max(last_seen, :ts),
                 battery_mv = CASE WHEN status_at IS NULL OR :ts >= status_at THEN :bat ELSE battery_mv END,
                 uptime_s = CASE WHEN status_at IS NULL OR :ts >= status_at THEN :up ELSE uptime_s END,
                 noise_floor_dbm = CASE WHEN status_at IS NULL OR :ts >= status_at THEN :noise ELSE noise_floor_dbm END,
                 status_at = max(coalesce(status_at, :ts), :ts)""",
            {"k": p["public_key"].lower(), "ts": p["ts"], "bat": p.get("battery_mv"), "up": p.get("uptime_s"),
             "noise": p.get("noise_floor_dbm")},
        )

    def _link(self, p: dict[str, Any]) -> None:
        self._db.execute(
            """INSERT INTO local_links (hop, ts, snr, rssi, count) VALUES (:hop, :ts, :snr, :rssi, :count)
               ON CONFLICT (hop) DO UPDATE SET ts = :ts, snr = :snr, rssi = :rssi, count = :count
               WHERE :ts >= local_links.ts""",
            {"hop": str(p["from"]).lower(), "ts": p["ts"], "snr": p.get("snr"), "rssi": p.get("rssi"),
             "count": p.get("count") or 0},
        )

    def _test(self, p: dict[str, Any]) -> None:
        self._db.execute(
            "INSERT INTO local_tests (ts, target, delivered, latency_ms, error) VALUES (?, ?, ?, ?, ?)",
            (p["ts"], p["target"], 1 if p.get("delivered") else 0, p.get("latency_ms"), p.get("error")),
        )
        self._db.execute(
            "DELETE FROM local_tests WHERE id NOT IN (SELECT id FROM local_tests ORDER BY ts DESC, id DESC LIMIT ?)",
            (MAX_TESTS,),
        )

    def _log(self, p: dict[str, Any]) -> None:
        self._db.execute(
            "INSERT INTO local_log (ts, channel, from_name, kind, content, lat, lng) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (p["ts"], p["channel"], p.get("from_name"), p["kind"], p.get("content"), p.get("lat"), p.get("lng")),
        )
        self._db.execute(
            "DELETE FROM local_log WHERE id NOT IN (SELECT id FROM local_log ORDER BY ts DESC, id DESC LIMIT ?)",
            (MAX_LOG,),
        )

    def snapshot(self, managed_names: dict[str, str | None], now: datetime | None = None) -> dict[str, Any]:
        """Vue lisible de l'état : nœuds (avec leur état), liaisons, derniers tests, main courante.

        managed_names : clé (ou début de clé) → nom donné dans la configuration, pour les relais administrés.
        """
        now = now or datetime.now(timezone.utc)
        names = {k.lower(): v for k, v in managed_names.items()}
        managed = list(names)
        nodes = []
        seen_managed: set[str] = set()
        for r in self._db.execute("SELECT * FROM local_nodes ORDER BY last_seen DESC"):
            key = r["public_key"]
            match = next((m for m in managed if key.startswith(m)), None)
            if match:
                seen_managed.add(match)
            silent = now - _parse(r["last_seen"]) > SILENT_AFTER
            low = r["battery_mv"] is not None and r["battery_mv"] < BATTERY_LOW_MV
            nodes.append({
                **dict(r), "name": r["name"] or (names[match] if match else None), "managed": match is not None,
                "state": "silent" if silent else "battery_low" if low else "ok",
            })
        for m in managed:
            if m not in seen_managed:
                nodes.append({"public_key": m, "name": names[m], "role": "repeater", "managed": True,
                              "last_seen": None, "state": "never_seen"})
        links = [dict(r) for r in self._db.execute("SELECT * FROM local_links ORDER BY ts DESC LIMIT 100")]
        tests = [{**dict(r), "delivered": bool(r["delivered"])}
                 for r in self._db.execute("SELECT ts, target, delivered, latency_ms, error FROM local_tests ORDER BY ts DESC, id DESC LIMIT 20")]
        log_rows = [dict(r) for r in self._db.execute(
            "SELECT ts, channel, from_name, kind, content, lat, lng FROM local_log ORDER BY ts DESC, id DESC LIMIT ?",
            (LOG_SHOWN,))]
        return {"nodes": nodes, "links": links, "tests": tests, "log": log_rows}

    def close(self) -> None:
        self._db.close()
