# SPDX-License-Identifier: Apache-2.0
"""Orchestration asyncio de l'agent passerelle.

Radio + écoute passive + interrogation des relais + relevé des capteurs + tampon SQLite + liaison MQTT (si configurée).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from . import __version__
from .buffer import Outbox
from .collectors import PassiveCollector, StatusPoller, TelemetryPoller, Topics, envelope
from .commands import CommandHandler
from .config import AgentConfig, load_config
from .local_state import LocalState
from .radio import MeshCoreRadio, Radio
from .simulator import SimulatedRadio
from .uplink import Uplink

if TYPE_CHECKING:
    from .local_api import Runtime

log = logging.getLogger("supervimesh_agent")
RECONNECT_DELAYS_S = [5, 10, 30, 60]
MANUAL_REFRESH_MIN_INTERVAL_S = 15 * 60  # budget radio : pas plus d'une actualisation manuelle par quart d'heure
TEST_MIN_INTERVAL_S = 10 * 60  # budget radio : au plus un test réseau toutes les 10 minutes
SEND_MIN_INTERVAL_S = 5  # budget radio : au plus un message de la console toutes les 5 secondes
TEST_TIMEOUT_S = 30


def build_radio(cfg: AgentConfig) -> Radio:
    if cfg.radio.transport == "simulator":
        return SimulatedRadio()
    return MeshCoreRadio(cfg.radio)


async def keep_radio_connected(radio: Radio) -> None:
    attempt = 0
    while True:
        if not radio.is_connected():
            try:
                await radio.connect()
                attempt = 0
            except Exception as exc:
                delay = RECONNECT_DELAYS_S[min(attempt, len(RECONNECT_DELAYS_S) - 1)]
                log.warning("radio indisponible (%s), nouvel essai dans %d s", exc, delay)
                attempt += 1
                await asyncio.sleep(delay)
                continue
        await asyncio.sleep(10)


async def listen(radio: Radio, collector: PassiveCollector) -> None:
    while True:
        collector.handle(await radio.events())


async def flush_links_every_minute(collector: PassiveCollector) -> None:
    while True:
        await asyncio.sleep(60)
        collector.flush_links(datetime.now(timezone.utc))


async def serve_local_api(cfg: AgentConfig, state: LocalState, runtime: "Runtime") -> None:
    """Tableau de bord local ; s'il ne peut pas démarrer (port occupé), l'agent continue sans lui."""
    import uvicorn

    from .local_api import create_app

    server = uvicorn.Server(uvicorn.Config(
        create_app(cfg, state, runtime), host=cfg.local_api.host, port=cfg.local_api.port,
        log_level="warning", access_log=False, server_header=False,
    ))
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]  # l'agent gère l'arrêt
    log.info("tableau de bord local sur http://%s:%d", cfg.local_api.host, cfg.local_api.port)
    try:
        await server.serve()
    except (OSError, SystemExit) as exc:
        log.error("tableau de bord local indisponible (%s) ; l'agent continue sans lui", exc)


async def run_agent(cfg: AgentConfig) -> None:
    outbox = Outbox(cfg.buffer_path)
    state = LocalState(cfg.buffer_path)
    outbox.add_listener(state.observe)
    radio = build_radio(cfg)
    collector = PassiveCollector(cfg, outbox, lambda: radio.self_key)
    poller = StatusPoller(cfg, outbox, radio)
    telemetry = TelemetryPoller(cfg, outbox, radio)
    last_refresh = [-MANUAL_REFRESH_MIN_INTERVAL_S]

    async def refresh_status() -> None:
        if time.monotonic() - last_refresh[0] < MANUAL_REFRESH_MIN_INTERVAL_S:
            log.info("actualisation manuelle ignorée (budget radio)")
            return
        last_refresh[0] = time.monotonic()
        for repeater in cfg.managed_repeaters:
            await poller.poll_once(repeater)

    last_test = [-TEST_MIN_INTERVAL_S]
    topic = Topics(cfg)

    async def run_test(test_id: str, target: str, request_id: str) -> str | None:
        if not radio.is_connected():
            return "radio déconnectée"
        if time.monotonic() - last_test[0] < TEST_MIN_INTERVAL_S:
            return "budget radio : test précédent trop récent"
        last_test[0] = time.monotonic()

        async def execute() -> None:
            outcome = await radio.run_test(target, TEST_TIMEOUT_S)
            outbox.enqueue(topic("test_result"), envelope(
                outcome.ts, test_id=test_id, request_id=request_id, target=target, delivered=outcome.delivered,
                latency_ms=outcome.latency_ms, path=outcome.path, error=outcome.error,
            ))

        asyncio.create_task(execute())
        return None

    last_send = [-SEND_MIN_INTERVAL_S]

    async def send_channel(channel: str, text: str) -> str | None:
        if not radio.is_connected():
            return "radio déconnectée"
        if time.monotonic() - last_send[0] < SEND_MIN_INTERVAL_S:
            return "budget radio : message précédent trop récent"
        last_send[0] = time.monotonic()
        return await radio.send_channel_message(channel, text)

    tasks = [
        keep_radio_connected(radio),
        listen(radio, collector),
        flush_links_every_minute(collector),
        *(poller.run_for(r) for r in cfg.managed_repeaters),
        *(telemetry.run_for(s) for s in cfg.sensors),
    ]
    uplink = Uplink(cfg, outbox, radio.is_connected, CommandHandler(cfg, refresh_status, run_test, send_channel)) if cfg.uplink else None
    if uplink:
        tasks.append(uplink.run())
    else:
        log.warning("aucun broker configuré : mode local seul, les messages restent dans le tampon")
    if cfg.local_api.enabled:
        from .local_api import Runtime

        tasks.append(serve_local_api(cfg, state, Runtime(
            radio_connected=radio.is_connected,
            cloud_connected=lambda: uplink.connected if uplink else None,
            pending=outbox.pending_count,
            self_key=lambda: radio.self_key,
        )))
    log.info("agent %s démarré, %d message(s) en attente", __version__, outbox.pending_count())
    try:
        await asyncio.gather(*tasks)
    finally:
        await radio.close()
        outbox.close()
        state.close()


def setup_logging(level: str, log_file: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.handlers.RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5))
    logging.basicConfig(level=level, handlers=handlers, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def run() -> None:
    parser = argparse.ArgumentParser(description="Agent passerelle SuperviMesh")
    parser.add_argument("-c", "--config", type=Path, default=Path("/etc/supervimesh/config.yaml"))
    parser.add_argument("--log-file", type=Path)
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(cfg.log_level, args.log_file)
    try:
        asyncio.run(run_agent(cfg))
    except KeyboardInterrupt:
        log.info("arrêt demandé")


if __name__ == "__main__":
    run()
