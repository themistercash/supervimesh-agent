# Spécification MQTT SuperviMesh — `schema_version` 1

> Contrat entre l'agent passerelle et le service d'ingestion. Source de vérité : les schémas JSON de
> `packages/mqtt-schemas/schemas/`. Toute modification incompatible passe par une nouvelle `schema_version`.
> Ce document est publié avec l'agent open source : chacun peut vérifier ce qui sort de son réseau.

## Principes
- MeshCore n'a pas de MQTT intégré : c'est l'agent qui publie vers **notre** broker. Jamais de broker public.
- JSON UTF-8, quelques centaines d'octets, 16 Ko maximum.
- Chaque message montant porte `schema_version`, `msg_id` (UUID, sert au dédoublonnage) et `ts`
  (ISO 8601 **UTC, suffixe `Z`**, heure de la mesure et non de l'envoi).
- Les messages mis en tampon pendant une coupure partent plus tard avec leur `ts` d'origine.
  L'ingestion est **idempotente** : un `msg_id` déjà reçu est ignoré.
- `{org}`, `{site}`, `{gw}` sont des identifiants techniques (`[a-z0-9-]`), jamais des noms de personnes.
- Les nœuds sont identifiés par leur clé publique MeshCore en hexadécimal minuscule. Un préfixe
  (2 caractères minimum) est accepté quand la radio ne fournit pas la clé complète (liaisons, chemins).
- Champ indisponible = **absent ou `null`**, jamais inventé. Tout champ inconnu est **refusé**
  (`additionalProperties: false`), sauf dans `node/status.extra`.

## Sujets montants (publiés par la passerelle)

| Sujet `mv/{org}/{site}/{gw}/…` | Schéma | Fréquence | QoS | Retenu |
|---|---|---|---|---|
| `heartbeat` | `heartbeat` | 60 s | 0 | non |
| `status` | `gateway_status` | connexion / déconnexion (testament) | 1 | **oui** |
| `node/advert` | `node_advert` | à chaque annonce | 1 | non |
| `node/status` | `node_status` | 30–60 min par relais administré | 1 | non |
| `link` | `link` | agrégé par minute | 0 | non |
| `test_result` | `test_result` | après chaque test | 1 | non |
| `event_log` | `event_log` | canaux d'exploitation activés par le client | 1 | non |
| `cmd/result` | `cmd_result` | après chaque commande | 1 | non |

`link.from` = dernier relais du chemin (préfixe de hash), `link.to` = clé du companion de la passerelle.

### `event_log` (main courante)
- **Refus par défaut** : seuls les canaux nommés dans la configuration locale (`event_log.channels`, par nom
  sans tenir compte de la casse ni des espaces de bord, ou par index `#0`, `#1`…) sont publiés. Jamais de
  message privé (l'agent n'écoute pas `CONTACT_MSG_RECV`).
- `channel` = nom du canal lu sur le companion, ou `#index` s'il est inconnu (64 caractères maximum).
- Un message de canal MeshCore **ne porte pas d'expéditeur** (ni clé, ni nom) : `from_key` vaut toujours
  `null` ; `from_name` est tiré du préfixe « Nom: » du texte (avant le premier `": "`, 64 caractères maximum),
  sinon `null`. Ce nom est déclaratif, non authentifié. `content` = le reste du texte, tronqué à 500 caractères.
- `kind` = `sos` si `content` commence par un mot-clé SOS (mot entier, casse ignorée, défaut `SOS`) ou contient
  `#sos` ; sinon `position` si `content` contient des coordonnées décimales ; sinon `message`.
- Coordonnées : `lat, lng` ou `lat lng` avec au moins 3 décimales de chaque côté, ou au moins 1 décimale après
  un préfixe `POS`, `GPS` ou `position` ; lat ∈ [-90, 90], lng ∈ [-180, 180]. Un `sos` garde aussi `lat`/`lng`.
- `ts` = heure d'émission déclarée par l'expéditeur si elle est à moins de 10 min (avant ou après) de la
  réception, sinon heure de réception par la passerelle.

## Sujet descendant (lu par la passerelle)

`mv/{org}/{site}/{gw}/cmd`, QoS 1, schéma `cmd`.
- Liste blanche : `run_test`, `refresh_status`, `update_config`, `ping`. Toute autre commande est refusée.
- Une commande dont `expires_at` est passé est ignorée (réponse `ok: false`, `error: "expired"`).
- `update_config` ne modifie que des paramètres non sensibles (intervalles d'interrogation, niveau de logs).
- Réponse sur `cmd/result` avec le même `request_id`.

## Ce qui ne transite JAMAIS par MQTT
Contenu des messages privés, mots de passe administrateur des relais, clés de chiffrement des canaux,
toute donnée personnelle inutile.

## Sécurité du broker
- TLS obligatoire (8883). Aucun port non chiffré exposé sur internet.
- Un identifiant + mot de passe par passerelle, généré par la plateforme, révocable.
- ACL : une passerelle publie uniquement sous `mv/{son org}/{son site}/{elle}/#` **sauf** `cmd`, et lit
  uniquement son `cmd`. Le service d'ingestion lit `mv/#` et publie uniquement sur les `cmd`.
- Taille maximale 16 Ko ; débit plafonné par plan (`plan_limits.mqtt_max_msgs_per_minute`) côté ingestion.
- L'ingestion considère chaque message comme **non fiable** : validation du schéma et cohérence du sujet
  avec le compte de la passerelle.

## Tests de contrat
`packages/mqtt-schemas/examples/valid/*.json` doivent être acceptés et `examples/invalid/*.json` refusés,
à la fois par les modèles pydantic de l'agent et par les schémas Zod de l'ingestion.
