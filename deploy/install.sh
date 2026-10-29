#!/usr/bin/env bash
# Installation de l'agent SuperviMesh sur Raspberry Pi OS (Debian). À lancer avec sudo.
set -euo pipefail
REPO="${SUPERVIMESH_REPO:-https://github.com/themistercash/supervimesh-agent}"
REF="${SUPERVIMESH_REF:-main}"

[ "$(id -u)" -eq 0 ] || { echo "Lancez ce script avec sudo."; exit 1; }
apt-get update -qq && apt-get install -y -qq python3 python3-venv git >/dev/null

id supervimesh >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin supervimesh
usermod -a -G dialout supervimesh
install -d -o supervimesh -g supervimesh -m 750 /var/lib/supervimesh /var/log/supervimesh
install -d -m 755 /etc/supervimesh /opt/supervimesh

rm -rf /opt/supervimesh/src && git clone -q --depth 1 --branch "$REF" "$REPO" /opt/supervimesh/src
python3 -m venv /opt/supervimesh/venv
/opt/supervimesh/venv/bin/pip install -q --upgrade pip
/opt/supervimesh/venv/bin/pip install -q /opt/supervimesh/src

if [ ! -f /etc/supervimesh/config.yaml ]; then
  install -m 640 -g supervimesh /opt/supervimesh/src/config.example.yaml /etc/supervimesh/config.yaml
  echo "Configuration à compléter : /etc/supervimesh/config.yaml"
fi
install -m 644 /opt/supervimesh/src/deploy/supervimesh-agent.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now supervimesh-agent
echo "Agent SuperviMesh installé. Journaux : journalctl -u supervimesh-agent -f"
