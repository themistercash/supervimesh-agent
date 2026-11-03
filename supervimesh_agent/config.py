# SPDX-License-Identifier: Apache-2.0
"""Chargement et validation du fichier de configuration YAML."""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class RadioConfig(BaseModel):
    transport: Literal["serial", "tcp", "simulator"] = "serial"
    port: str = "/dev/ttyUSB0"
    baudrate: int = 115200
    host: str | None = None
    tcp_port: int = 5000

    @model_validator(mode="after")
    def _tcp_needs_host(self) -> "RadioConfig":
        if self.transport == "tcp" and not self.host:
            raise ValueError("radio.host est obligatoire avec transport: tcp")
        return self


class ManagedRepeater(BaseModel):
    """Relais administré : son mot de passe reste dans ce fichier local, jamais envoyé au cloud."""

    public_key: str = Field(pattern=r"^[0-9a-fA-F]{12,64}$")
    password: str = Field(repr=False)
    name: str | None = None


class Sensor(BaseModel):
    """Capteur MeshCore dont la passerelle relève la télémétrie (CayenneLPP).

    Le mot de passe (invité ou administrateur, seulement si le capteur l'exige) reste dans ce fichier local,
    jamais envoyé au cloud.
    """

    public_key: str = Field(pattern=r"^[0-9a-fA-F]{12,64}$")
    name: str | None = None
    password: str | None = Field(default=None, repr=False)
    interval_min: int = Field(default=15, ge=5)  # budget radio : au plus un relevé toutes les 5 min


class PollingConfig(BaseModel):
    """Budget radio : une demande de statut par relais toutes les min..max minutes (gigue aléatoire)."""

    status_min_interval_min: int = Field(default=30, ge=15)
    status_max_interval_min: int = Field(default=60, ge=15)
    request_timeout_s: float = Field(default=20, gt=0)

    @model_validator(mode="after")
    def _order(self) -> "PollingConfig":
        if self.status_max_interval_min < self.status_min_interval_min:
            raise ValueError("status_max_interval_min doit être ≥ status_min_interval_min")
        return self


class IdentityConfig(BaseModel):
    """Identifiants techniques utilisés dans les sujets MQTT (jamais des noms de personnes)."""

    org: str = Field(pattern=r"^[a-z0-9-]{1,64}$")
    site: str = Field(pattern=r"^[a-z0-9-]{1,64}$")
    gateway: str = Field(pattern=r"^[a-z0-9-]{1,64}$")


class UplinkConfig(BaseModel):
    """Broker MQTT sur TLS. Identifiants propres à cette passerelle, générés par la plateforme."""

    host: str
    port: int = 8883
    username: str
    password: str = Field(repr=False)
    ca_file: Path | None = None  # None = autorités de certification du système (Let's Encrypt)
    heartbeat_interval_s: int = Field(default=60, ge=10)


class LocalApiConfig(BaseModel):
    """Mini tableau de bord en lecture seule sur le réseau local (aucun port à ouvrir vers internet)."""

    enabled: bool = True
    host: str = "0.0.0.0"  # 127.0.0.1 = visible seulement depuis la passerelle elle-même
    port: int = Field(default=8080, ge=1, le=65535)


class EventLogConfig(BaseModel):
    """Main courante : canaux d'exploitation relayés vers la plateforme. Liste vide = rien n'est relayé.

    Une entrée est un nom de canal (casse et espaces de bord ignorés) ou un index « #0 », « #1 »…
    Jamais de messages privés : seuls les messages de canal sont concernés.
    """

    channels: list[str] = []
    sos_keywords: list[str] = ["SOS"]

    def allows(self, channel_idx: int, channel_name: str | None) -> bool:
        wanted = {c.strip().casefold() for c in self.channels if c.strip()}
        if f"#{channel_idx}" in wanted:
            return True
        return channel_name is not None and channel_name.strip().casefold() in wanted


class AgentConfig(BaseModel):
    identity: IdentityConfig
    uplink: UplinkConfig | None = None  # None = mode local seul (aucun envoi au cloud)
    radio: RadioConfig = RadioConfig()
    polling: PollingConfig = PollingConfig()
    managed_repeaters: list[ManagedRepeater] = []
    sensors: list[Sensor] = []
    local_api: LocalApiConfig = LocalApiConfig()
    event_log: EventLogConfig = EventLogConfig()
    buffer_path: Path = Path("/var/lib/supervimesh/buffer.db")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


def load_config(path: Path) -> AgentConfig:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return AgentConfig.model_validate(data)
