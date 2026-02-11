# Pipeline NYC Taxi

Ingestion des données NYC taxi (yellow cab) dans une base **DuckDB** (fichier local, pas de serveur).

## Prérequis

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) pour les dépendances

## Installation

```bash
cd pipeline
uv sync
```

## Ingestion des données

Créer/écraser la base DuckDB et remplir la table à partir des CSV (par défaut : janvier 2021) :

```bash
uv run python ingest_data.py
```

Options utiles :

- `--db-path` : chemin du fichier DuckDB (défaut : `ny_taxi.duckdb`)
- `--year` / `--month` : année et mois des données
- `--target-table` : nom de la table (défaut : `yellow_taxi_data`)
- `--chunksize` : taille des chunks CSV (défaut : 100000)

Exemples :

```bash
# Fichier DB personnalisé
uv run python ingest_data.py --db-path ./data/taxi.duckdb

# Autre période
uv run python ingest_data.py --year 2022 --month 3
```

## Interroger la base

DuckDB est embarqué : pas de serveur à lancer. Vous pouvez :

- Utiliser le notebook Marimo `upload_data.py` (engine déjà configuré sur `ny_taxi.duckdb`)
- Ouvrir le fichier avec [DuckDB CLI](https://duckdb.org/docs/installation/) : `duckdb ny_taxi.duckdb`

## Notebook Marimo

Le projet utilise [Marimo](https://marimo.io/) à la place de Jupyter : notebook réactif en Python pur (`.py`), compatible Git.

**Lancer le notebook :**

```bash
uv run marimo edit upload_data.py
```

Une fenêtre du navigateur s’ouvre. Chaque modification de cellule déclenche automatiquement la réexécution des cellules qui en dépendent.

**Autres commandes utiles :**

```bash
# Lancer en mode app (code masqué, interactif)
uv run marimo run upload_data.py

# Exécuter comme script Python
uv run python upload_data.py
```
