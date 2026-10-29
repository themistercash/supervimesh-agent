# SPDX-License-Identifier: Apache-2.0
"""Orchestration asyncio de l'agent passerelle.

Radio + écoute passive + interrogation des relais + tampon SQLite + liaison MQTT (si configurée).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import time
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .buffer import Outbox
from .collectors import PassiveCollector, StatusPoller, Topics, envelope
from .commands import CommandHandler
from .config import AgentConfig, load_config
from .radio import MeshCoreRadio, Radio
from .simulator import SimulatedRadio
from .uplink import Uplink

log = logging.getLogger("supervimesh_agent")
RECONNECT_DELAYS_S = [5, 10, 30, 60]
MANUAL_REFRESH_MIN_INTERVAL_S = 15 * 60  # budget radio : pas plus d'une actualisation manuelle par quart d'heure
TEST_MIN_INTERVAL_S = 10 * 60  # budget radio : au plus un test réseau toutes les 10 minutes
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


async def run_agent(cfg: AgentConfig) -> None:
    outbox = Outbox(cfg.buffer_path)
    radio = build_radio(cfg)
    collector = PassiveCollector(cfg, outbox, lambda: radio.self_key)
    poller = StatusPoller(cfg, outbox, radio)
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

    tasks = [
        keep_radio_connected(radio),
        listen(radio, collector),
        flush_links_every_minute(collector),
        *(poller.run_for(r) for r in cfg.managed_repeaters),
    ]
    if cfg.uplink:
        tasks.append(Uplink(cfg, outbox, radio.is_connected, CommandHandler(cfg, refresh_status, run_test)).run())
    else:
        log.warning("aucun broker configuré : mode local seul, les messages restent dans le tampon")
    log.info("agent %s démarré, %d message(s) en attente", __version__, outbox.pending_count())
    try:
        await asyncio.gather(*tasks)
    finally:
        await radio.close()
        outbox.close()


def setup_logging(level: str, log_file: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.handlers.RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5))
    logging.basicConfig(level=level, handlers=handlers, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def run() -> None:
    parser = argparse.ArgumentParser(description="Agent passerelle SuperviMesh")
    parser.add_argument("-c", "--config", type=Path, default=Path("/etc/supervimesh/config.yaml"))
    parser.add_argument("--log-file", type=Path)
    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(cfg.log_level, args.log_file)
    try:
        asyncio.run(run_agent(cfg))
    except KeyboardInterrupt:
        log.info("arrêt demandé")


if __name__ == "__main__":
    run()
