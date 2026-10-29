# SPDX-License-Identifier: Apache-2.0
"""Modèles pydantic des messages MQTT (schema_version 1).

Miroir des schémas JSON de packages/mqtt-schemas/schemas : la parité est vérifiée par tests/test_contract.py.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

NodeKey = Annotated[str, Field(pattern=r"^[0-9a-f]{2,64}$")]
UtcTs = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")]
Lat = Annotated[float | None, Field(ge=-90, le=90)]
Lng = Annotated[float | None, Field(ge=-180, le=180)]
OptNum = float | None
OptInt = StrictInt | None

CMD_WHITELIST = ("run_test", "refresh_status", "update_config", "ping")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Envelope(_Strict):
    schema_version: Literal[1]
    msg_id: Annotated[str, Field(pattern=r"^[0-9a-fA-F-]{36}$")]
    ts: UtcTs


class Heartbeat(_Envelope):
    agent_version: Annotated[str, Field(max_length=32)]
    uptime_s: Annotated[StrictInt, Field(ge=0)]
    radio_connected: StrictBool
    buffer_pending: Annotated[StrictInt, Field(ge=0)]
    cpu_temp_c: OptNum = None
    disk_free_mb: OptInt = None


class NodeAdvert(_Envelope):
    public_key: NodeKey
    name: Annotated[str | None, Field(max_length=64)] = None
    role: Literal["repeater", "room_server", "companion", "sensor", "unknown"]
    lat: Lat = None
    lng: Lng = None
    rssi: OptNum = None
    snr: OptNum = None
    hops: OptInt = None


class NodeStatus(_Envelope):
    public_key: NodeKey
    battery_mv: OptInt = None
    uptime_s: OptInt = None
    noise_floor_dbm: OptNum = None
    airtime_tx_s: OptNum = None
    packets_rx: OptInt = None
    packets_tx: OptInt = None
    extra: dict[str, Union[float, str, bool, None]] | None = None


class Link(_Envelope):
    from_: NodeKey = Field(alias="from")
    to: NodeKey | None = None
    snr: OptNum = None
    rssi: OptNum = None
    count: Annotated[StrictInt, Field(ge=1)]


class TestResult(_Envelope):
    test_id: Annotated[str, Field(max_length=64)]
    request_id: Annotated[str | None, Field(max_length=64)] = None
    target: NodeKey
    delivered: StrictBool
    latency_ms: OptInt = None
    path: Annotated[list[NodeKey], Field(max_length=64)] | None = None
    error: Annotated[str | None, Field(max_length=500)] = None


class EventLog(_Envelope):
    channel: Annotated[str, Field(max_length=64)]
    from_name: Annotated[str | None, Field(max_length=64)] = None
    from_key: NodeKey | None = None
    kind: Literal["message", "sos", "position"]
    content: Annotated[str | None, Field(max_length=500)] = None
    lat: Lat = None
    lng: Lng = None


class CmdResult(_Envelope):
    request_id: Annotated[str, Field(max_length=64)]
    ok: StrictBool
    error: Annotated[str | None, Field(max_length=500)] = None


class Cmd(_Strict):
    schema_version: Literal[1]
    request_id: Annotated[str, Field(max_length=64)]
    cmd: Literal["run_test", "refresh_status", "update_config", "ping"]
    args: dict[str, Any] | None = None
    expires_at: UtcTs


class GatewayStatus(_Strict):
    schema_version: Literal[1]
    state: Literal["online", "offline"]
    ts: UtcTs | None = None


# Nom du schéma JSON → modèle
MODELS: dict[str, type[BaseModel]] = {
    "heartbeat": Heartbeat,
    "node_advert": NodeAdvert,
    "node_status": NodeStatus,
    "link": Link,
    "test_result": TestResult,
    "event_log": EventLog,
    "cmd_result": CmdResult,
    "cmd": Cmd,
    "gateway_status": GatewayStatus,
}

# Suffixe de sujet montant → nom du schéma
UPLINK_TOPICS: dict[str, str] = {
    "heartbeat": "heartbeat",
    "status": "gateway_status",
    "node/advert": "node_advert",
    "node/status": "node_status",
    "link": "link",
    "test_result": "test_result",
    "event_log": "event_log",
    "cmd/result": "cmd_result",
}


def validate_outgoing(topic_suffix: str, payload: dict[str, Any]) -> None:
    """Lève une ValidationError si l'agent s'apprête à publier un message hors contrat."""
    MODELS[UPLINK_TOPICS[topic_suffix]].model_validate(payload)
