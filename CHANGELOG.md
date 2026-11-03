# Journal des modifications

## 0.5.0

- Télémétrie des capteurs MeshCore (CayenneLPP) : nouvelle section `sensors` de la configuration (clé publique,
  nom, intervalle de 15 min par défaut, 5 min au minimum, avec un léger délai aléatoire). Les mesures sont publiées
  sur `node/telemetry` (QoS 1, via le tampon). Seules les valeurs numériques finies sont gardées, 32 au plus.
- Mot de passe invité/administrateur facultatif par capteur : connexion seulement s'il est renseigné ; il reste
  sur la passerelle, jamais envoyé au cloud.
- Simulateur : capteur « Capteur Col » (tension, température, humidité).

## 0.4.0

- Commande `send_channel_msg` : la console d'un événement peut répondre sur la radio. Envoi seulement sur un canal
  déjà autorisé dans la section `event_log` de la configuration, une ligne de 120 caractères au plus, un message
  toutes les 5 secondes au plus. Jamais de message privé.

## 0.3.1

- `install.sh` sert aussi de mise à jour : il redémarre l'agent et affiche l'ancienne et la nouvelle version.
- Option `--auto-update` : mise à jour automatique chaque lundi vers 4 h (minuteur systemd, désactivable avec `--no-auto-update`).
- `supervimesh-agent --version`.

## 0.3.0 — 2026-10-30
- Main courante (pack Événement) : les messages des **canaux d'exploitation explicitement autorisés**
  (section `event_log` de la configuration, refus par défaut) sont publiés sur `event_log` (QoS 1).
  Expéditeur tiré du préfixe « Nom: », détection SOS (mot-clé en tête ou `#sos`) et position (coordonnées
  décimales). Jamais de messages privés : `CONTACT_MSG_RECV` n'est pas écouté.
- Tableau de bord local : section « Main courante » (30 derniers messages, SOS mis en évidence).
- Simulateur : messages sur les canaux « Public » (#0) et « Organisation » (#1).

## 0.2.0 — 2026-10-29
- Mode local : mini tableau de bord en lecture seule (FastAPI) sur le port 8080 du réseau local,
  disponible sans internet ; état du réseau conservé sur la passerelle (SQLite).
- Le script d'installation refuse de s'exécuter ailleurs que sur la passerelle.

## 0.1.0 — 2026-10-29
- Première version publique : écoute passive, statut planifié des relais, tests réseau (découverte de chemin),
  tampon SQLite, liaison MQTT TLS (heartbeat, statut retenu, testament), commandes en liste blanche, simulateur.
