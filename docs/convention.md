# Convention de nommage

## Table des matières
- [Convention de nommage](#convention-de-nommage)
  - [Table des matières](#table-des-matières)
  - [Dags](#dags)
  - [S3](#s3)
  - [SQL](#sql)
  - [Grist](#grist)


## Dags

Tous les dags doivent êtres organisés de la façon suivante:
```
.
├── dags
│   ├── projet_X
│   │   ├── grist
│   │   │   ├── sql   # Contient les scripts sql
│   │   │   │   └── projet_X.sql
│   │   │   ├── dags.py   # Définition du dag
│   │   │   ├── process.py  # Contient toutes les fonctions de processing
│   │   │   ├── actions.py   # Contient les actions à réaliser dans le dag
│   │   │   ├── enums.py   # (Optionnel) Enums propres au dag
│   │   │   ├── entities.py   # (Optionnel) Types propres au dag
│   │   │   ├── readme.md   # Documentation du dag
│   │   │   └── tasks.py  # Contient les tâches spécifiques au dag
```

## S3

Les buckets S3 sont organisés par niveau. Il existe un total de 4 niveaux:
- Niveau 1: FONCTION (ex: Finance, RH, Marketing)
- Niveau 2: ACTIVITÉ (ex: Comptabilité, Paie, Recrutement)
- Niveau 3: PROCESSUS/TRANSACTION (ex: Factures, Contrats)
- Niveau 4: FONCTION DOSSIER/SÉRIE (ex: 2024, Client_X)

```
bucket/niveau_1/niveau_2/niveau_3/niveau_4/file.ext
# Exemple 1: bucket/immobilier/energie/consommation/20250801/13h30/conso.parquet
# Exemple 2: bucket/finances/budget/ht2/20250801/13h30/achat.parquet
```
Dans les exemples, le niveau 4 est composé de la date et de l'heure au format `/AAAAMMDD/HHhMM`

Avec Iceberg, le niveau 4 n'est pas nécessaire car il sera géré via les partitions si nécessaires.  
Il rajoute une profondeur, automatiquement, entre le bucket et le niveau 1 qui correspond au nom du catalog
```
bucket/niveau_1/niveau_2/niveau_3/nom_table
# Exemple 1: bucket/catalog/immobilier/energie/consommation/conso
```

## SQL
1. Les tables

Application de ces [règles](https://www.postgresql.org/docs/7.0/syntax525.htm).
Le nom des tables doit définis avec les agents métiers.

2. Les vues

Les vues suivent les même règles que les tables.
En complément, le suffix `_vw` doit être rajouté pour faciliter la recherche.

3. Les colonnes

Les colonnes suivent les même règles que les tables.
En complément, si une même information est présente dans plusieurs tables, il faut s'assurer de conserver le même nom de colonne par cohérence.

## Grist
Ces conventions visent à faciliter le traitement des données issues de Grist.
- Les tables de documentations

Ajouter le préfix `doc_` => `doc_nom_de_la_table`

- Les tables métiers

Aucun préfix ou suffix nécessaire. Il faut néanmoins qu'elles possèdent un nom clair et concis qui découlent de la structuration des données définies dans le projet.

- Les tables de référence

Ajouter le préfix `ref_` => `ref_direction`

- Les tables d'onglets

Ajouter le préfix `onglet_` => `onglet_global_dsci`
Les onglets permettent de hiérarchiser visuellement les tables sur le bandeau latéral gauche. Elles ne contiennent pas de données mais peuvent être utilisées pour de la documentation.

- Les colonnes intermédiaires

Toutes les colonnes qui n'ont pas de significations métiers mais qui servent pour mettre en place certaines fonctionnalités
Si c'est une colonne qui servira pour un questionnaire => `quest_nom_colonne`
Si c'est une colonne de traitement intermédiaire => `int_nom_colonne`

> Cette convention de nommage n'empêche pas l'utilisation de label plus explicite pour les utilisateurs finaux.
