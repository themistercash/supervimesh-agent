# SuperviMesh Agent

Agent passerelle open source (Apache 2.0) pour superviser un réseau radio [MeshCore](https://github.com/meshcore-dev/MeshCore) :
il écoute le réseau depuis un boîtier *companion* branché à un Raspberry Pi, interroge les relais avec
parcimonie et envoie l'état du réseau à un broker MQTT (par défaut la plateforme SuperviMesh).

Vous pouvez vérifier ici, ligne par ligne, **tout ce qui sort de votre réseau** : la liste exacte des
messages est dans [`docs/mqtt-spec.md`](docs/mqtt-spec.md) et les schémas dans [`mqtt-schemas/`](mqtt-schemas/).

## Ce que fait l'agent
- Écoute passive : annonces des nœuds, qualité des liaisons (SNR, RSSI) — aucune émission radio.
- Statut des relais que vous administrez (batterie, bruit, temps d'émission) : **au plus une demande toutes les 30 à 60 min par relais**, avec un délai aléatoire, pour respecter le temps d'émission en 868 MHz.
- Télémétrie des capteurs MeshCore que vous déclarez (température, humidité, tension, position…) : **au plus un relevé toutes les 15 min par capteur** par défaut.
- Tests réseau à la demande de la plateforme (au plus un toutes les 10 min).
- Tampon local SQLite : rien n'est perdu pendant une coupure internet.
- Connexions **sortantes uniquement**, en TLS. Aucun port à ouvrir chez vous.
- **Mode local** : un mini tableau de bord en lecture seule sur votre réseau local, même sans internet.
- **Main courante** (option, désactivée par défaut) : relais des messages des canaux d'exploitation que vous autorisez.

## Ce qui ne sort JAMAIS
Contenu des messages privés, messages des canaux que vous n'avez pas autorisés, mots de passe administrateur des relais et des capteurs (ils restent dans votre fichier
de configuration local), clés de chiffrement des canaux.

## Canaux d'exploitation (main courante)
Pendant un événement, l'agent peut relayer vers la plateforme les messages d'un ou plusieurs **canaux
MeshCore d'exploitation** (organisation, sécurité). **Rien n'est relayé par défaut** : il faut nommer
chaque canal dans la section `event_log` de la configuration, par son nom (casse ignorée) ou son index :
```yaml
event_log:
  channels: ["Organisation"]   # ou "#1"
  sos_keywords: ["SOS"]
```
- **Consentement** : n'ajoutez que des canaux d'exploitation dont les membres savent que leurs messages
  sont enregistrés. Jamais de canal public.
- **Messages privés** : jamais relayés, quelle que soit la configuration (l'agent ne les écoute pas).
- MeshCore ne transmet pas l'expéditeur d'un message de canal : le nom affiché vient du préfixe
  « Nom: » du texte (convention des applications MeshCore) et n'est donc pas authentifié.
- Un message qui commence par un mot-clé SOS (ou contient `#sos`) devient une alerte ; des coordonnées
  décimales (`45.123, 5.456`, ou `POS 45.1,5.7`) sont reprises comme position.
- Les 30 derniers messages relayés s'affichent aussi dans le tableau de bord local (« Main courante »).
- La clé de chiffrement des canaux n'est ni conservée, ni journalisée, ni transmise : seul le nom du canal est lu.

## Capteurs (télémétrie)
L'agent relève la télémétrie CayenneLPP des capteurs MeshCore listés dans la section `sensors` de la
configuration, et seulement ceux-là :
```yaml
sensors:
  - public_key: d4e5f6a1b2c3   # 12 caractères hexadécimaux au moins
    name: Capteur Col          # facultatif
    interval_min: 15           # minimum 5 ; un léger délai aléatoire s'ajoute
    password: "…"              # facultatif : seulement si le capteur exige une connexion invité/administrateur
```
- Le mot de passe reste sur la passerelle : il n'est ni journalisé ni envoyé au cloud.
- Seules les mesures numériques sont transmises (32 au plus par relevé) ; un relevé vide n'est pas publié.
- Pas de relevé quand la radio est déconnectée ; une erreur radio est journalisée sans arrêter l'agent.

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

## Mise à jour

Relancer la commande d'installation met l'agent à jour (configuration et tampon conservés) :

```bash
curl -fsSL https://raw.githubusercontent.com/themistercash/supervimesh-agent/main/deploy/install.sh | sudo bash
```

Mise à jour automatique chaque lundi vers 4 h (facultative) : ajoutez `-s -- --auto-update` après `sudo bash`
(`--no-auto-update` pour la désactiver). La version installée s'affiche avec `supervimesh-agent --version` et sur la
page Passerelles de la plateforme, qui signale quand une version plus récente existe.

## Tableau de bord local
Depuis un ordinateur ou un téléphone du même réseau : `http://<nom-du-pi>.local:8080`
(ou `http://localhost:8080` sur la passerelle). Il affiche l'état de la radio et du cloud, les messages
en attente d'envoi, les nœuds entendus (actif, batterie faible, muet depuis plus de 2 h, jamais vu) et les
derniers tests réseau, et la main courante si elle est activée. Il fonctionne **sans internet** : l'état est gardé sur la passerelle.

Lecture seule : aucune commande radio, aucun mot de passe affiché, aucune ressource externe.
Réglages dans la section `local_api` de la configuration (`host: 127.0.0.1` pour le réserver à la
passerelle, `enabled: false` pour le couper). Ne l'exposez pas sur internet.

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
