# SPDX-License-Identifier: Apache-2.0
"""Mini tableau de bord local (FastAPI), consultable sur le réseau local même sans internet.

Lecture seule : aucune commande radio, aucun mot de passe exposé. Page autonome (aucune ressource externe).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response

from . import __version__
from .config import AgentConfig
from .local_state import LocalState


@dataclass
class Runtime:
    """Informations vivantes de l'agent, lues à chaque requête."""

    radio_connected: Callable[[], bool]
    cloud_connected: Callable[[], bool | None]  # None = aucun broker configuré
    pending: Callable[[], int]
    self_key: Callable[[], str | None]
    started: float = field(default_factory=time.monotonic)


SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
                               "img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


def create_app(cfg: AgentConfig, state: LocalState, runtime: Runtime) -> FastAPI:
    # Routes asynchrones : elles tournent sur la boucle de l'agent, seul fil autorisé à lire SQLite.
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def headers(request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        response.headers.update(SECURITY_HEADERS)
        return response

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return PAGE

    @app.get("/app.js")
    async def script() -> Response:
        return Response(SCRIPT, media_type="text/javascript; charset=utf-8")

    @app.get("/style.css")
    async def style() -> Response:
        return Response(STYLE, media_type="text/css; charset=utf-8")

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/state")
    async def api_state() -> dict:
        i = cfg.identity
        return {
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "gateway": {
                "org": i.org, "site": i.site, "gateway": i.gateway, "agent_version": __version__,
                "uptime_s": int(time.monotonic() - runtime.started),
                "radio_connected": runtime.radio_connected(),
                "cloud_connected": runtime.cloud_connected(),
                "buffer_pending": runtime.pending(),
                "self_key": runtime.self_key(),
            },
            **state.snapshot({r.public_key: r.name for r in cfg.managed_repeaters}),
        }

    return app


PAGE = """<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SuperviMesh · passerelle</title>
<link rel="stylesheet" href="/style.css">
<script src="/app.js" defer></script>
</head>
<body>
<header>
  <h1>SuperviMesh <span>tableau de bord local</span></h1>
  <p id="ident">Chargement…</p>
</header>
<main>
  <section class="cards" aria-label="État de la passerelle">
    <div class="card"><h2>Radio</h2><p id="radio">—</p></div>
    <div class="card"><h2>Cloud</h2><p id="cloud">—</p></div>
    <div class="card"><h2>En attente d'envoi</h2><p id="pending">—</p></div>
    <div class="card"><h2>Nœuds</h2><p id="counts">—</p></div>
  </section>
  <section>
    <h2>Nœuds entendus</h2>
    <div class="scroll"><table>
      <thead><tr><th>État</th><th>Nom</th><th>Rôle</th><th>Vu</th><th>Batterie</th><th>Signal</th><th>Sauts</th><th>Clé</th></tr></thead>
      <tbody id="nodes"></tbody>
    </table></div>
  </section>
  <section>
    <h2>Main courante</h2>
    <div class="scroll"><table>
      <thead><tr><th>Heure</th><th>Canal</th><th>De</th><th>Type</th><th>Message</th><th>Position</th></tr></thead>
      <tbody id="log"></tbody>
    </table></div>
  </section>
  <section>
    <h2>Derniers tests réseau</h2>
    <div class="scroll"><table>
      <thead><tr><th>Date</th><th>Cible</th><th>Résultat</th><th>Délai</th></tr></thead>
      <tbody id="tests"></tbody>
    </table></div>
  </section>
  <p class="foot" id="foot">Actualisation automatique toutes les 15 s. Cette page fonctionne même sans internet.</p>
</main>
</body>
</html>
"""

STYLE = """
:root { --bg:#f6f7f9; --fg:#1b1f24; --muted:#5d6670; --card:#fff; --line:#e3e6ea;
  --ok:#1a7f37; --warn:#9a6700; --bad:#cf222e; --idle:#6e7781; }
@media (prefers-color-scheme: dark) { :root { --bg:#0f1216; --fg:#e6e8eb; --muted:#9aa3ad; --card:#181c21;
  --line:#2b3138; --ok:#3fb950; --warn:#d29922; --bad:#f85149; --idle:#8b949e; } }
* { box-sizing: border-box; }
body { margin:0; font:15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; background:var(--bg); color:var(--fg); }
header, main { max-width:1100px; margin:0 auto; padding:16px; }
h1 { font-size:20px; margin:8px 0 2px; } h1 span { font-weight:400; color:var(--muted); font-size:15px; }
h2 { font-size:15px; margin:20px 0 8px; }
#ident { margin:0; color:var(--muted); }
.cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:12px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px 14px; }
.card h2 { margin:0 0 4px; font-size:13px; color:var(--muted); font-weight:500; }
.card p { margin:0; font-size:18px; font-weight:600; }
.scroll { overflow-x:auto; background:var(--card); border:1px solid var(--line); border-radius:10px; }
table { width:100%; border-collapse:collapse; }
th, td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); white-space:nowrap; }
th { font-size:12px; color:var(--muted); font-weight:500; }
tr:last-child td { border-bottom:0; }
.pill { display:inline-block; padding:1px 8px; border-radius:99px; font-size:12px; font-weight:600; color:#fff; }
.ok { background:var(--ok); } .warn { background:var(--warn); } .bad { background:var(--bad); } .idle { background:var(--idle); }
.t-ok { color:var(--ok); } .t-bad { color:var(--bad); } .t-warn { color:var(--warn); }
.mono { font-family:ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px; color:var(--muted); }
.star { color:var(--warn); }
.foot { color:var(--muted); font-size:13px; }
.empty { color:var(--muted); text-align:center; padding:18px; }
tr.sos td { background:color-mix(in srgb, var(--bad) 14%, transparent); font-weight:600; }
td.wrap { white-space:normal; min-width:220px; }
"""

# Les données viennent de la radio (noms de nœuds) : construites avec textContent, jamais en HTML.
SCRIPT = r"""
"use strict";
const $ = (id) => document.getElementById(id);
const STATES = { ok: ["Actif", "ok"], battery_low: ["Batterie faible", "warn"], silent: ["Muet", "bad"], never_seen: ["Jamais vu", "idle"] };
const KINDS = { message: ["Message", "idle"], position: ["Position", "ok"], sos: ["SOS", "bad"] };
const ROLES = { repeater: "Relais", companion: "Compagnon", room_server: "Salon", sensor: "Capteur" };

function ago(iso, now) {
  if (!iso) return "jamais";
  const s = Math.max(0, (Date.parse(now) - Date.parse(iso)) / 1000);
  if (s < 60) return "à l'instant";
  if (s < 3600) return `il y a ${Math.floor(s / 60)} min`;
  if (s < 86400) return `il y a ${Math.floor(s / 3600)} h`;
  return `il y a ${Math.floor(s / 86400)} j`;
}
function fmtDate(iso) { return new Date(iso).toLocaleString("fr-FR", { dateStyle: "short", timeStyle: "short" }); }
function cell(tr, text, cls) { const td = document.createElement("td"); td.textContent = text ?? "—"; if (cls) td.className = cls; tr.appendChild(td); return td; }
function setStatus(el, text, cls) { el.textContent = text; el.className = cls; }
function empty(tbody, cols, text) { const tr = tbody.insertRow(); const td = cell(tr, text, "empty"); td.colSpan = cols; }

function render(d) {
  const g = d.gateway;
  $("ident").textContent = `Organisation ${g.org} · site ${g.site} · passerelle ${g.gateway} · agent ${g.agent_version}`;
  setStatus($("radio"), g.radio_connected ? "Connectée" : "Déconnectée", g.radio_connected ? "t-ok" : "t-bad");
  if (g.cloud_connected === null) setStatus($("cloud"), "Non configuré (local seul)", "t-warn");
  else setStatus($("cloud"), g.cloud_connected ? "Connecté" : "Hors ligne", g.cloud_connected ? "t-ok" : "t-bad");
  $("pending").textContent = `${g.buffer_pending} message${g.buffer_pending > 1 ? "s" : ""}`;
  const silent = d.nodes.filter((n) => n.state === "silent" || n.state === "never_seen").length;
  $("counts").textContent = `${d.nodes.length} dont ${silent} muet${silent > 1 ? "s" : ""}`;

  const nodes = $("nodes"); nodes.replaceChildren();
  if (!d.nodes.length) empty(nodes, 8, "Aucun nœud entendu pour l'instant.");
  for (const n of d.nodes) {
    const tr = nodes.insertRow();
    const [label, cls] = STATES[n.state] ?? ["?", "idle"];
    const pill = document.createElement("span"); pill.className = `pill ${cls}`; pill.textContent = label;
    cell(tr, "").appendChild(pill);
    const name = cell(tr, n.name ?? "(sans nom)");
    if (n.managed) { const s = document.createElement("span"); s.className = "star"; s.textContent = " ★"; s.title = "Relais administré"; name.appendChild(s); }
    cell(tr, ROLES[n.role] ?? n.role);
    cell(tr, ago(n.last_seen, d.now));
    cell(tr, n.battery_mv != null ? `${(n.battery_mv / 1000).toFixed(2)} V` : null);
    cell(tr, n.snr != null || n.rssi != null ? `${n.snr ?? "—"} dB · ${n.rssi ?? "—"} dBm` : null);
    cell(tr, n.hops != null ? String(n.hops) : null);
    cell(tr, n.public_key.slice(0, 12), "mono");
  }

  const log = $("log"); log.replaceChildren();
  if (!d.log.length) empty(log, 6, "Aucun message des canaux d'exploitation (aucun canal autorisé, ou rien reçu).");
  for (const e of d.log) {
    const tr = log.insertRow();
    if (e.kind === "sos") tr.className = "sos";
    const [label, cls] = KINDS[e.kind] ?? ["?", "idle"];
    cell(tr, fmtDate(e.ts));
    cell(tr, e.channel);
    cell(tr, e.from_name ?? "(inconnu)");
    const pill = document.createElement("span"); pill.className = `pill ${cls}`; pill.textContent = label;
    cell(tr, "").appendChild(pill);
    cell(tr, e.content, "wrap");
    cell(tr, e.lat != null && e.lng != null ? `${e.lat.toFixed(5)}, ${e.lng.toFixed(5)}` : null, "mono");
  }

  const tests = $("tests"); tests.replaceChildren();
  if (!d.tests.length) empty(tests, 4, "Aucun test réseau pour l'instant.");
  for (const t of d.tests) {
    const tr = tests.insertRow();
    cell(tr, fmtDate(t.ts));
    cell(tr, t.target.slice(0, 12), "mono");
    cell(tr, t.delivered ? "Réussi" : `Échec${t.error ? " : " + t.error : ""}`, t.delivered ? "t-ok" : "t-bad");
    cell(tr, t.latency_ms != null ? `${(t.latency_ms / 1000).toFixed(1)} s` : null);
  }
  $("foot").textContent = `Mis à jour ${fmtDate(d.now)}. Actualisation automatique toutes les 15 s. Cette page fonctionne même sans internet.`;
}

async function refresh() {
  try {
    const r = await fetch("/api/state", { cache: "no-store" });
    if (!r.ok) throw new Error(r.status);
    render(await r.json());
  } catch {
    $("foot").textContent = "Agent injoignable : nouvel essai dans 15 s.";
  }
}
refresh();
setInterval(refresh, 15000);
"""
