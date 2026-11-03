# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from supervimesh_agent.buffer import Outbox
from supervimesh_agent.collectors import envelope
from supervimesh_agent.local_api import Runtime, create_app
from supervimesh_agent.local_state import LocalState

KEY = "a1b2c3d4e5f6" + "0" * 52  # clé publique MeshCore fictive, pas un secret  gitleaks:allow
OTHER = "b2" * 32
BASE = "mv/org-1/site-1/gw-1"
NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def advert(key: str, ts: datetime, **fields):
    return envelope(ts, public_key=key, name=fields.pop("name", "Relais Église"), role="repeater",
                    lat=45.1, lng=5.7, rssi=-98, snr=7.5, hops=0, **fields)


def wired(cfg):
    outbox = Outbox(cfg.buffer_path)
    state = LocalState(cfg.buffer_path)
    outbox.add_listener(state.observe)
    return outbox, state


def test_state_follows_outbox_and_survives_restart(cfg):
    outbox, state = wired(cfg)
    outbox.enqueue(f"{BASE}/node/advert", advert(KEY, NOW - timedelta(minutes=5)))
    outbox.enqueue(f"{BASE}/node/status", envelope(NOW, public_key=KEY, battery_mv=3300, uptime_s=10, noise_floor_dbm=-112))
    outbox.ack([m.row_id for m in outbox.peek()])  # envoyé au cloud : l'état local reste
    state.close()

    snap = LocalState(cfg.buffer_path).snapshot({KEY[:12]: None}, NOW)
    [node] = snap["nodes"]
    assert node["name"] == "Relais Église" and node["battery_mv"] == 3300
    assert node["managed"] is True and node["state"] == "battery_low"
    assert node["last_seen"] == "2026-10-29T12:00:00Z"


def test_old_message_does_not_overwrite_newer(cfg):
    _, state = wired(cfg)
    state.observe(f"{BASE}/node/advert", advert(KEY, NOW, name="Nom récent"))
    state.observe(f"{BASE}/node/advert", advert(KEY, NOW - timedelta(hours=3), name="Nom ancien"))
    [node] = state.snapshot({}, NOW)["nodes"]
    assert node["name"] == "Nom récent" and node["last_seen"] == "2026-10-29T12:00:00Z"


def test_states_silent_and_never_seen(cfg):
    _, state = wired(cfg)
    state.observe(f"{BASE}/node/advert", advert(OTHER, NOW - timedelta(hours=3)))
    nodes = {n["public_key"]: n for n in state.snapshot({KEY: "Relais test"}, NOW)["nodes"]}
    assert nodes[OTHER]["state"] == "silent"
    assert nodes[KEY.lower()]["state"] == "never_seen" and nodes[KEY.lower()]["name"] == "Relais test"


def test_bad_message_never_blocks_enqueue(cfg):
    outbox, state = wired(cfg)
    assert outbox.enqueue(f"{BASE}/node/advert", {"msg_id": "x", "ts": "2026-10-29T12:00:00Z"})  # sans clé
    assert state.snapshot({}, NOW)["nodes"] == []


def test_tests_are_kept_and_trimmed(cfg):
    _, state = wired(cfg)
    for i in range(60):
        state.observe(f"{BASE}/test_result", envelope(NOW + timedelta(minutes=i), test_id="t", request_id="r",
                                                      target=KEY, delivered=i % 2 == 0, latency_ms=1200,
                                                      path=None, error=None if i % 2 == 0 else "délai dépassé"))
    tests = state.snapshot({}, NOW)["tests"]
    assert len(tests) == 20 and tests[0]["ts"] == "2026-10-29T12:59:00Z" and tests[0]["delivered"] is False


def client(cfg, cloud=None):
    outbox, state = wired(cfg)
    outbox.enqueue(f"{BASE}/node/advert", advert(KEY, NOW, name="<img src=x onerror=alert(1)>"))
    runtime = Runtime(radio_connected=lambda: True, cloud_connected=lambda: cloud,
                      pending=outbox.pending_count, self_key=lambda: "f0" * 32)
    return TestClient(create_app(cfg, state, runtime))


def test_api_state_without_secrets(cfg):
    r = client(cfg).get("/api/state")
    assert r.status_code == 200
    body = r.json()
    assert body["gateway"]["gateway"] == "gw-1" and body["gateway"]["cloud_connected"] is None
    assert body["gateway"]["buffer_pending"] == 1 and len(body["nodes"]) == 1
    assert "secret" not in r.text  # mot de passe du relais administré (conftest)


def test_page_is_self_contained_and_hardened(cfg):
    c = client(cfg, cloud=False)
    page = c.get("/")
    assert page.status_code == 200 and "tableau de bord local" in page.text
    assert "http" not in page.text.replace('lang="fr"', "")  # aucune ressource externe : marche sans internet
    csp = page.headers["content-security-policy"]
    assert "script-src 'self'" in csp and "unsafe-inline" not in csp
    js = c.get("/app.js").text
    assert "innerHTML" not in js  # les noms de nœuds viennent de la radio : jamais interprétés en HTML
    assert c.get("/style.css").status_code == 200
    assert c.get("/docs").status_code == 404 and c.get("/openapi.json").status_code == 404


def test_read_only(cfg):
    c = client(cfg)
    assert c.post("/api/state").status_code == 405


def log_entry(ts: datetime, **fields):
    base = dict(channel="Organisation", from_name="Ravito km 18", from_key=None, kind="message",
                content="RAS", lat=None, lng=None)
    return envelope(ts, **{**base, **fields})


def test_event_log_kept_trimmed_and_does_not_count_as_node(cfg):
    outbox, state = wired(cfg)
    for i in range(210):
        outbox.enqueue(f"{BASE}/event_log", log_entry(NOW + timedelta(seconds=i), content=f"m{i}"))
    outbox.enqueue(f"{BASE}/event_log", log_entry(NOW + timedelta(hours=1), kind="sos", content="SOS coureur",
                                                  lat=45.2, lng=5.7))
    snap = state.snapshot({}, NOW)
    assert len(snap["log"]) == 30 and snap["nodes"] == []
    assert snap["log"][0]["kind"] == "sos" and snap["log"][0]["lat"] == 45.2 and snap["log"][1]["content"] == "m209"
    assert state._db.execute("SELECT count(*) FROM local_log").fetchone()[0] == 200


def test_api_exposes_log_section_without_html(cfg):
    outbox, state = wired(cfg)
    outbox.enqueue(f"{BASE}/event_log", log_entry(NOW, kind="sos", content="<script>alert(1)</script>"))
    runtime = Runtime(radio_connected=lambda: True, cloud_connected=lambda: None,
                      pending=outbox.pending_count, self_key=lambda: None)
    c = TestClient(create_app(cfg, state, runtime))
    body = c.get("/api/state").json()
    assert body["log"][0]["content"] == "<script>alert(1)</script>" and body["log"][0]["kind"] == "sos"
    assert "Main courante" in c.get("/").text
    js = c.get("/app.js").text
    assert "innerHTML" not in js and "outerHTML" not in js and "insertAdjacentHTML" not in js
    assert 'tr.className = "sos"' in js
