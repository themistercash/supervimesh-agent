# SPDX-License-Identifier: Apache-2.0
"""Commandes descendantes : liste blanche, expiration, aucune commande système."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
import re
from typing import Awaitable, Callable

from pydantic import ValidationError

from .config import AgentConfig, PollingConfig
from .schemas import Cmd

log = logging.getLogger(__name__)

# Seuls ces paramètres non sensibles peuvent être changés à distance.
UPDATABLE_POLLING = {"status_min_interval_min", "status_max_interval_min"}
LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


TEST_ID = re.compile(r"^[0-9a-zA-Z-]{1,64}$")
NODE_KEY = re.compile(r"^[0-9a-f]{12,64}$")

# Lance un test réseau en arrière-plan ; renvoie une erreur (str) s'il ne peut pas démarrer.
RunTest = Callable[[str, str, str], Awaitable[str | None]]


async def _no_test(test_id: str, target: str, request_id: str) -> str | None:
    return "tests réseau indisponibles"


# Envoie un message sur un canal de la radio ; renvoie l'erreur (str), None si envoyé.
SendChannel = Callable[[str, str], Awaitable[str | None]]
MAX_SEND_TEXT = 120


async def _no_send(channel: str, text: str) -> str | None:
    return "envoi de messages indisponible"


class CommandHandler:
    def __init__(
        self,
        cfg: AgentConfig,
        refresh_status: Callable[[], Awaitable[None]],
        run_test: RunTest = _no_test,
        send_channel: SendChannel = _no_send,
    ) -> None:
        self._cfg = cfg
        self._refresh_status = refresh_status
        self._run_test = run_test
        self._send_channel = send_channel

    async def handle(self, raw: bytes, now: datetime | None = None) -> tuple[str | None, bool, str | None]:
        """Renvoie (request_id, ok, erreur). request_id est None si le message est illisible."""
        try:
            cmd = Cmd.model_validate(json.loads(raw))
        except (ValueError, ValidationError) as exc:
            log.warning("commande refusée : %s", str(exc).splitlines()[0])
            request_id = _peek_request_id(raw)
            return request_id, False, "commande invalide ou non autorisée"
        now = now or datetime.now(timezone.utc)
        if datetime.fromisoformat(cmd.expires_at.replace("Z", "+00:00")) <= now:
            return cmd.request_id, False, "expired"
        args = cmd.args or {}
        if cmd.cmd == "ping":
            return cmd.request_id, True, None
        if cmd.cmd == "refresh_status":
            await self._refresh_status()
            return cmd.request_id, True, None
        if cmd.cmd == "update_config":
            return cmd.request_id, *self._update_config(args)
        if cmd.cmd == "send_channel_msg":
            return cmd.request_id, *await self._send(args)
        # run_test : arguments contrôlés, le test tourne en arrière-plan et publie test_result.
        test_id, target = args.get("test_id"), args.get("target")
        if not (isinstance(test_id, str) and TEST_ID.match(test_id) and isinstance(target, str) and NODE_KEY.match(target)):
            return cmd.request_id, False, "arguments de test invalides"
        error = await self._run_test(test_id, target, cmd.request_id)
        return cmd.request_id, error is None, error

    async def _send(self, args: dict) -> tuple[bool, str | None]:
        """Message de la console de l'événement : seulement sur un canal déjà autorisé pour la main courante."""
        channel, text = args.get("channel"), args.get("text")
        if not (isinstance(channel, str) and 0 < len(channel.strip()) <= 64):
            return False, "canal invalide"
        if not (isinstance(text, str) and 0 < len(text.strip()) <= MAX_SEND_TEXT and "\n" not in text and "\r" not in text):
            return False, "message invalide"
        allowed = {c.strip().casefold() for c in self._cfg.event_log.channels if c.strip()}
        if channel.strip().casefold() not in allowed:
            return False, "canal non autorisé dans la configuration (event_log)"
        error = await self._send_channel(channel.strip(), text.strip())
        return error is None, error

    def _update_config(self, args: dict) -> tuple[bool, str | None]:
        unknown = set(args) - UPDATABLE_POLLING - {"log_level"}
        if unknown:
            return False, f"paramètres non modifiables : {', '.join(sorted(unknown))}"
        if "log_level" in args:
            if args["log_level"] not in LOG_LEVELS:
                return False, "log_level invalide"
            logging.getLogger("supervimesh_agent").setLevel(args["log_level"])
            self._cfg.log_level = args["log_level"]
        polling = {k: v for k, v in args.items() if k in UPDATABLE_POLLING}
        if polling:
            try:
                self._cfg.polling = PollingConfig.model_validate({**self._cfg.polling.model_dump(), **polling})
            except ValidationError:
                return False, "intervalles refusés (minimum 15 min, max ≥ min)"
        return True, None


def _peek_request_id(raw: bytes) -> str | None:
    try:
        rid = json.loads(raw).get("request_id")
    except (ValueError, AttributeError):
        return None
    return rid if isinstance(rid, str) and len(rid) <= 64 else None
