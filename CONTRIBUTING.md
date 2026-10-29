# Contribuer

Merci ! Les contributions sont bienvenues, en particulier le **support de nouveaux matériels** MeshCore.

1. Ouvrez d'abord un ticket pour discuter du changement.
2. Le code est en Python 3.11+, typé, avec des tests (`pytest`) utilisant le simulateur radio.
3. Toute modification du format des messages passe par une nouvelle `schema_version` (voir `docs/mqtt-spec.md`).
4. L'agent ne doit **jamais** inonder le réseau radio : toute nouvelle requête radio respecte un budget.
5. Aucun secret, identifiant ou adresse interne dans le code ou les tests.

En contribuant, vous acceptez que votre contribution soit publiée sous licence Apache 2.0.
