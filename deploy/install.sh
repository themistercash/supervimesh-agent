#!/usr/bin/env bash
# Installation ET mise à jour de l'agent SuperviMesh sur Raspberry Pi OS (Debian). À lancer avec sudo.
# Relancer le script met l'agent à jour ; la configuration (/etc/supervimesh) et le tampon sont conservés.
# Option --auto-update : mise à jour automatique chaque lundi vers 4 h (désactivable : --no-auto-update).
set -euo pipefail
REPO="${SUPERVIMESH_REPO:-https://github.com/themistercash/supervimesh-agent}"
REF="${SUPERVIMESH_REF:-main}"

[ "$(uname -s)" = "Linux" ] && command -v apt-get >/dev/null && command -v systemctl >/dev/null || {
  echo "Ce script s'installe sur la passerelle (Raspberry Pi OS ou Debian), pas sur cet ordinateur."
  echo "Connectez-vous d'abord au Raspberry Pi (ssh <utilisateur>@<nom-du-pi>.local), puis relancez-le."
  exit 1
}
[ "$(id -u)" -eq 0 ] || { echo "Lancez ce script avec sudo."; exit 1; }
AUTO=""
for arg in "$@"; do
  case "$arg" in
    --auto-update) AUTO=on ;;
    --no-auto-update) AUTO=off ;;
    *) echo "Option inconnue : $arg"; exit 1 ;;
  esac
done
OLD="$(/opt/supervimesh/venv/bin/supervimesh-agent --version 2>/dev/null || true)"
apt-get update -qq
apt-get install -y -qq python3 python3-venv git curl >/dev/null

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
install -m 644 /opt/supervimesh/src/deploy/supervimesh-update.service /opt/supervimesh/src/deploy/supervimesh-update.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable supervimesh-agent
systemctl restart supervimesh-agent   # prend la nouvelle version si c'est une mise à jour
case "$AUTO" in
  on) systemctl enable --now supervimesh-update.timer; echo "Mise à jour automatique activée (chaque lundi vers 4 h)." ;;
  off) systemctl disable --now supervimesh-update.timer 2>/dev/null || true; echo "Mise à jour automatique désactivée." ;;
esac
NEW="$(/opt/supervimesh/venv/bin/supervimesh-agent --version 2>/dev/null || echo "?")"
if [ -n "$OLD" ]; then echo "Agent SuperviMesh mis à jour : $OLD → $NEW."; else echo "Agent SuperviMesh $NEW installé."; fi
echo "Journaux : journalctl -u supervimesh-agent -f"
