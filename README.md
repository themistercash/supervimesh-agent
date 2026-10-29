# SuperviMesh Agent

Agent passerelle open source (Apache 2.0) pour superviser un réseau radio [MeshCore](https://github.com/meshcore-dev/MeshCore) :
il écoute le réseau depuis un boîtier *companion* branché à un Raspberry Pi, interroge les relais avec
parcimonie et envoie l'état du réseau à un broker MQTT (par défaut la plateforme SuperviMesh).

Vous pouvez vérifier ici, ligne par ligne, **tout ce qui sort de votre réseau** : la liste exacte des
messages est dans [`docs/mqtt-spec.md`](docs/mqtt-spec.md) et les schémas dans [`mqtt-schemas/`](mqtt-schemas/).

## Ce que fait l'agent
- Écoute passive : annonces des nœuds, qualité des liaisons (SNR, RSSI) — aucune émission radio.
- Statut des relais que vous administrez (batterie, bruit, temps d'émission) : **au plus une demande toutes les 30 à 60 min par relais**, avec un délai aléatoire, pour respecter le temps d'émission en 868 MHz.
- Tests réseau à la demande de la plateforme (au plus un toutes les 10 min).
- Tampon local SQLite : rien n'est perdu pendant une coupure internet.
- Connexions **sortantes uniquement**, en TLS. Aucun port à ouvrir chez vous.

## Ce qui ne sort JAMAIS
Contenu des messages privés, mots de passe administrateur des relais (ils restent dans votre fichier
de configuration local), clés de chiffrement des canaux.

## Matériel
Raspberry Pi 4 ou 5 + boîtier MeshCore flashé en **Companion USB** (par exemple Heltec V3/V4).
Réglage pour la France : préréglage « EU/UK (Narrow) ».

## Installation (Raspberry Pi OS)
```bash
curl -fsSL https://raw.githubusercontent.com/themistercash/supervimesh-agent/main/deploy/install.sh | sudo bash
```
Puis modifiez `/etc/supervimesh/config.yaml` (identifiants fournis par la plateforme) et :
```bash
sudo systemctl restart supervimesh-agent && journalctl -u supervimesh-agent -f
```

## Sans matériel (simulateur)
```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/supervimesh-agent -c config.dev.yaml
.venv/bin/pytest
```

## Dépannage
- « Aucune réponse du boîtier » : le firmware doit être **Companion USB** (pas Bluetooth, pas Repeater).
- Port série : `ls /dev/ttyUSB* /dev/ttyACM*`, puis `radio.port` dans la configuration.
- Le broker refuse la connexion : vérifiez identifiants et `ca_file`.

Licence : Apache 2.0. Le nom et le logo SuperviMesh ne sont pas couverts par la licence ([TRADEMARKS.md](TRADEMARKS.md)).
