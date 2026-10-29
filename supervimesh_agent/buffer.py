# SPDX-License-Identifier: Apache-2.0
"""File d'attente SQLite des messages sortants : rien n'est perdu pendant une coupure internet."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msg_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    qos INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


@dataclass(frozen=True)
class PendingMessage:
    row_id: int
    topic: str
    qos: int
    payload: dict[str, Any]


class Outbox:
    def __init__(self, path: Path | str) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path))
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)

    def enqueue(self, topic: str, payload: dict[str, Any], qos: int = 1) -> bool:
        """Ajoute un message ; renvoie False si ce msg_id est déjà en attente (doublon)."""
        cur = self._db.execute(
            "INSERT OR IGNORE INTO outbox (msg_id, topic, qos, payload) VALUES (?, ?, ?, ?)",
            (payload["msg_id"], topic, qos, json.dumps(payload, ensure_ascii=False)),
        )
        self._db.commit()
        return cur.rowcount == 1

    def peek(self, limit: int = 50) -> list[PendingMessage]:
        rows = self._db.execute(
            "SELECT id, topic, qos, payload FROM outbox ORDER BY id LIMIT ?", (limit,)
        ).fetchall()
        return [PendingMessage(r[0], r[1], r[2], json.loads(r[3])) for r in rows]

    def ack(self, row_ids: list[int]) -> None:
        self._db.executemany("DELETE FROM outbox WHERE id = ?", [(i,) for i in row_ids])
        self._db.commit()

    def pending_count(self) -> int:
        return int(self._db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    def close(self) -> None:
        self._db.close()
