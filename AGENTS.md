# AGENTS.md — Airflow dags and data architecture

## Codebase structure

```
.
├── src
│    ├── _types: types transverses
│    ├── common_tasks: tâches génériques réutilisables
│    ├── dags: Contient l'ensemble des dags
│    ├── _enums: enums transverses
│    ├── infra: Code pour interagir avec l'infrastructure / systèmes externes
│    ├── utils: Code réutilisable (variables globales, tâches, fonctions ...)
│    └── src.constants.py: variables globales
├── docs: Contient toute la documentation du projet
├── test: Contient tous les tests unitaires
└── scripts: Contient différents scripts utilitaires
```

## Dags organisation

Dags organisation is described in [docs/dags.md](docs/dags.md).

