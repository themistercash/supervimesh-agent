# SPDX-License-Identifier: Apache-2.0
"""Liaison MQTT sur TLS vers le broker : vidage du tampon, heartbeat, statut retenu, commandes."""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import aiomqtt

from . import __version__
from .buffer import Outbox
from .collectors import Topics, envelope, iso
from .commands import CommandHandler
from .config import AgentConfig
from .schemas import validate_outgoing

log = logging.getLogger(__name__)
RECONNECT_DELAYS_S = [2, 5, 15, 30, 60]


def _dump(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def status_payload(state: str) -> bytes:
    return _dump({"schema_version": 1, "state": state, "ts": iso(datetime.now(timezone.utc))})


def _cpu_temp_c() -> float | None:
    try:
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            return round(int(f.read()) / 1000, 1)
    except (OSError, ValueError):
        return None


class Uplink:
    def __init__(
        self,
        cfg: AgentConfig,
        outbox: Outbox,
        radio_connected: Callable[[], bool],
        commands: CommandHandler,
    ) -> None:
        assert cfg.uplink is not None
        self._cfg = cfg
        self._up = cfg.uplink
        self._outbox = outbox
        self._radio_connected = radio_connected
        self._commands = commands
        self._topic = Topics(cfg)
        self._started = time.monotonic()
        self._wakeup = asyncio.Event()
        self.connected = False

    def notify(self) -> None:
        """Signale qu'un nouveau message attend dans le tampon."""
        self._wakeup.set()

    def _client(self) -> aiomqtt.Client:
        tls = aiomqtt.TLSParameters(ca_certs=str(self._up.ca_file) if self._up.ca_file else None)
        return aiomqtt.Client(
            self._up.host,
            self._up.port,
            username=self._up.username,
            password=self._up.password,
            identifier=f"supervimesh-{self._cfg.identity.gateway}-{uuid.uuid4().hex[:6]}",
            tls_params=tls,
            will=aiomqtt.Will(self._topic("status"), status_payload("offline"), qos=1, retain=True),
            keepalive=60,
        )

    async def run(self) -> None:
        attempt = 0
        while True:
            try:
                async with self._client() as client:
                    attempt = 0
                    self.connected = True
                    log.info("connecté au broker %s:%d", self._up.host, self._up.port)
                    await client.publish(self._topic("status"), status_payload("online"), qos=1, retain=True)
                    await client.subscribe(self._topic("cmd"), qos=1)
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._drain(client))
                        tg.create_task(self._heartbeat(client))
                        tg.create_task(self._listen(client))
            except* (aiomqtt.MqttError, OSError) as eg:
                self.connected = False
                delay = RECONNECT_DELAYS_S[min(attempt, len(RECONNECT_DELAYS_S) - 1)]
                log.warning("broker indisponible (%s), nouvel essai dans %d s", eg.exceptions[0], delay)
                attempt += 1
                await asyncio.sleep(delay)

    async def _drain(self, client: aiomqtt.Client) -> None:
        while True:
            batch = self._outbox.peek(50)
            if not batch:
                self._wakeup.clear()
                try:
                    await asyncio.wait_for(self._wakeup.wait(), timeout=5)
                except TimeoutError:
                    pass
                continue
            sent: list[int] = []
            try:
                for msg in batch:
                    suffix = msg.topic.split("/", 4)[4]
                    try:
                        validate_outgoing(suffix, msg.payload)
                    except ValueError:
                        log.error("message hors contrat supprimé du tampon : %s", msg.topic)
                        sent.append(msg.row_id)
                        continue
                    await client.publish(msg.topic, _dump(msg.payload), qos=msg.qos)
                    sent.append(msg.row_id)
            finally:
                # On ne retire du tampon que ce qui a été remis au broker.
                self._outbox.ack(sent)

    def heartbeat_payload(self) -> dict[str, Any]:
        disk = shutil.disk_usage(self._cfg.buffer_path.parent if self._cfg.buffer_path.parent.exists() else "/")
        return envelope(
            datetime.now(timezone.utc),
            agent_version=__version__,
            uptime_s=int(time.monotonic() - self._started),
            radio_connected=self._radio_connected(),
            buffer_pending=self._outbox.pending_count(),
            cpu_temp_c=_cpu_temp_c(),
            disk_free_mb=disk.free // 1_000_000,
        )

    async def _heartbeat(self, client: aiomqtt.Client) -> None:
        # Le heartbeat n'est pas mis en tampon : un signe de vie en retard n'a pas de sens.
        while True:
            payload = self.heartbeat_payload()
            validate_outgoing("heartbeat", payload)
            await client.publish(self._topic("heartbeat"), _dump(payload), qos=0)
            await asyncio.sleep(self._up.heartbeat_interval_s)

    async def _listen(self, client: aiomqtt.Client) -> None:
        async for message in client.messages:
            raw = message.payload if isinstance(message.payload, bytes) else str(message.payload).encode()
            request_id, ok, error = await self._commands.handle(raw)
            if request_id is None:
                continue
            result = envelope(datetime.now(timezone.utc), request_id=request_id, ok=ok, error=error)
            self._outbox.enqueue(self._topic("cmd/result"), result)
            self.notify()
