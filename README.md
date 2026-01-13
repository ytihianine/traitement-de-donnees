# Traitement des données

Ce repo contient des scripts et dags permettant de traiter des données.

## Organisation du repo

```
.
├── dags: Contient l'ensemble des dags
├── docs: Contient toute la documentation du repo
├── entities: types transverses
├── enums: enums transverses
├── infra: Code pour interagir avec l'infrastructure / systèmes externes
├── scripts: Contient différents scripts utilitaires
└── utils: Code réutilisable (variables globales, tâches, fonctions ...)
```

Le code est organisé de façon modulaire et réutilisable. L'objectif est de développer uniquement les éléments spécifiques à chaque traitement (les logiques métiers).


## Documentation

L'ensemble de la documentation se trouve dans le dossier `docs/`.

```
.
├── docs # Contient toute la documentation du repo
|    ├── configuration_projet.md # Gestion de la configuration des projets
|    ├── contribuer.md # Mise en place de l'environnement de dev
|    ├── convention.md # Description des différentes configurations
|    ├── dags.md # Comment mettre en place un dags
|    ├── grist.md # Mener un projet Grist
|    ├── infra.md # Description pour interagir avec les services externes
|    └── utilitaires.md # Description des éléments génériques disponibles
```

## Mise en place de l'infrastructure

Pour déployer l'infrastructure, voir le repo [https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/deploiement-applications](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/deploiement-applications)
