# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

import pytest

from supervimesh_agent.config import AgentConfig


@pytest.fixture
def cfg(tmp_path):
    return AgentConfig.model_validate({
        "identity": {"org": "org-1", "site": "site-1", "gateway": "gw-1"},
        "radio": {"transport": "simulator"},
        "managed_repeaters": [{"public_key": "a1b2c3d4e5f6" + "0" * 52, "password": "secret"}],
        "buffer_path": str(tmp_path / "buffer.db"),
    })


def schemas_root() -> Path:
    """Schémas MQTT partagés : mqtt-schemas/ (dépôt public) ou packages/mqtt-schemas/ (monorepo)."""
    here = Path(__file__).resolve()
    for candidate in (here.parents[1] / "mqtt-schemas", here.parents[2] / "packages" / "mqtt-schemas"):
        if candidate.exists():
            return candidate
    return here.parents[1] / "mqtt-schemas"
