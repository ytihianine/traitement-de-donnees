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
├── docs
|    ├── configuration_projet.md
|    ├── contribuer.md
|    ├── convention.md
|    ├── dags.md
|    ├── grist.md
|    ├── infra.md
|    └── utilitaires.md
```

## Mise en place de l'infrastructure

Pour déployer l'infrastructure, voir le repo [https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/deploiement-applications](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/deploiement-applications)
