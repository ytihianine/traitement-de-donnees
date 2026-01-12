## Réaliser un projet avec Grist

Cette documentation décrit le processus d'un projet Grist, de la phase de cadrage du projet à l'exploitation des données avec ChartsGouv

## Table des matières
- [Réaliser un projet avec Grist](#realiser-un-projet-avec-grist)
  - [Table des matières](#table-des-matières)
  - [Convention de nommage](#convention-de-nommage)
  - [Mener un projet Grist](#mener-un-projet-grist)

## Convention de nommage

Voir la section Grist de la documentation [convention.md](./convention.md#grist)

## Mener un projet Grist

1) Cadrer le besoin métier

Comme tout projet informatique, une première réunion de cadrage est nécessaire. Elle doit permettre de définir à minima:
- Les objectifs que le document Grist doit couvrir
- Les fonctionnalités attendues et le processus dans lequel le document Grist s'intègre.

Cette première réunion doit aussi permettre de savoir d'où proviennent les données dans le processus actuel et qui sont les acteurs impliqués.

2) Lister l'ensemble des données nécessaires

Si les données proviennent de fichiers plats ou de fichiers issues de SI, lister les colonnes présentent dans chacun des fichiers.
Si c'est un nouveau projet, les agents métiers doivent fournir la liste selon leurs besoins. Un atelier peut être réalisé pour aider à la définition des données.

3) Structurer les données

A partir de la liste des données, celles-ci doivent être regroupées par entité logique/métier. Il faut respecter au mieux la 3ème normalisation des données.
Cette étape est la plus importante et nécessite d'itérer avec l'agent métier.

4) Création du document Grist

Les tables de référentiels doivent être regroupées dans un onglet onglet_referentiel.
__ajouter_image__
Toutes les tables métiers doivent être dans un format table par défaut dans un onglet onglet_structure. L'objectif est de pouvoir travailler et modifier facilement les tables sans avoir à travailler depuis des widgets/pages spécifiques.

__ajouter_image__

Une fois le document créé dans Grist. Les tables peuvent être générés au format dbml.
L'outil suivant permet de réaliser cette transposition: [https://github.com/ytihianine/grist-doc-to-db-parser](https://github.com/ytihianine/grist-doc-to-db-parser)
Une fois le fichier dbml généré, vous pouvez copier/coller le contenu sur le site [https://dbdiagram.io/](https://dbdiagram.io/) et exporter le résultat pour PosteSQL.
