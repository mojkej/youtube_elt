# youtube_elt

## Résumé

---

Ce projet est un pipeline ELT (Extract, Load, Transform) qui collecte les données YouTube du youtubeur français Squeezie, les stocke dans une base PostgreSQL (schéma `staging` puis `core`) et propose des vérifications de qualité. L'objectif final (avec un data scientist) est de construire un modèle permettant d'estimer/prédire le chiffre d'affaires généré par une chaîne YouTube à partir des métriques publiques (vues, likes, durée, fréquence, etc.).

## Architecture

---

- Extraction des vidéos via l'API YouTube.
- Sauvegarde des résultats au format CSV.
- Chargement dans le schéma `staging` (données brutes ou peu transformées).
- Transformation vers le schéma `core` (types nettoyés, durées converties, features calculées).
- Contrôles de qualité (SODA ou checks personnalisés).
- Exécution orchestrée par Apache Airflow (DAGs).
- Base de données PostgreSQL comme source persistante.
- Conteneurisation via Docker (images Airflow, Postgres).

## Illustration de l'architecture

- Exemple d'insertion :
  ![Architecture](/docs/architecture.jpg)

## Données — exemples

---

- Exemple de données du schéma `staging` (données brutes ou proches de la source) :
  ![Staging sample](/docs/staging_sample.png)

- Exemple de données du schéma `core` (types propres, colonne duration convertie, features supplémentaires) :
  ![Core sample](/docs/core_sample.png)

## Airflow & Docker — aperçu

---

- Capture montrant les conteneurs Docker et l'UI Airflow :
  ![Docker](/docs/docker_ui.png)
  ![Airflowr](/docs/airflow_ui.png)

## Exemple Postman

---

- Capture de la requête Postman utilisée pour tester la récupération des données :
  ![Postman](/docs/postman.png)

## But du projet

---

Le but est d'extraire et préparer les données YouTube pour ensuite, avec un data scientist :

- Définir une variable cible (ex. CA estimé) : on peut approximer le CA par une métrique comme vues * CPM estimé, puis affiner avec données additionnelles si disponibles.
- Construire un jeu d'entraînement avec features temporelles, engagement, durée, catégorisation du contenu, etc.
- Entraîner et déployer un modèle prédictif (régression ou modèles plus complexes).

## Prérequis

---

- Docker & Docker Compose (pour exécution containerisée).
- Python 3.8+ (si exécution locale sans conteneurs).
- Airflow (si non via Docker) — version compatible avec vos providers.
- Clé API YouTube (configurer via Airflow Connection ou variable d'environnement).
- Connection PostgreSQL déclarée dans Airflow (conn id: `postgres_db_yt_elt`).

## Configuration importante

---

- Variables/Connections Airflow à définir :
  - Connection `postgres_db_yt_elt` → données Postgres (hôte, port, user, password, dbname).
  - Variable ou Connection pour la clé YouTube API.
  - Autres variables : noms de schémas (`staging`, `core`), chemins S3/GCS si vous migrez les CSV.
- Exemple de nommage du CSV : `./data/youtube_videos_Squeezie_YYYY-MM-DD.csv` (géré par `data_loading.py`).

## Exécution locale rapide (avec Docker)

---

1. Placer les images et fichiers requis (CSV) dans le dépôt, ajouter les connexions Airflow (via UI ou variables d'environnement).
2. Lancer les services (exemple si vous avez un docker-compose adapté) :
   - docker-compose up -d
3. Dans l'UI Airflow :
   - lancer le DAG `produce_csv_youtube` pour lancer l'extraction.
   - Le DAG `produce_csv_youtube` déclenche ensuite `update_db`, puis les checks de qualité.

## Bonnes pratiques pour la production

---

- Stocker les CSV/intermédiaires dans S3/GCS (éviter stockage local).
- Utiliser un exécuteur Airflow robuste (Kubernetes ou Celery) en production.
- Gérer secrets via un secrets backend (Vault, GCP Secret Manager, AWS Secrets Manager).
- CI/CD : tests unitaires, scanning de sécurité, build d'images Docker et déploiement automatisé.
- Monitoring & alerting pour les DAGs (SLAs, métriques d'échec).

## Prochaines étapes pour la partie ML (prévision de CA)

---

1. Définir précisément la cible (CA journalier/hebdomadaire/mois par vidéo).
2. Enrichir les données (tags, catégories, tendances, historique de la chaîne).
