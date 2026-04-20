# Documentation

### Règles appliquées
#### S3
- Suppression des fichiers pour les keys `*/tmp*`

Suppression pure et simple de toutes les keys qui comportent le segment `*/tmp*` dans la key.

- Suppresion des keys qui comporte une date `*/YYYYMMJJ/*`

Pour le mois actuel, on conserve tous les fichiers.  
Pour les mois précédents, on ne conserve que le dernier fichier disponible du mois.

#### Airflow logs

- Suppresion des logs des tâches



### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `src.dags.py`     |
| Description | Ce traitement permet de nettoyer les logs des systèmes externes (db, s3, Airflow ...) |
| Fréquence de mise à jour | Quotidien |
| Fonctionnement | Automatisé |
| Propriétaires des données | MEF - SG - DSCI |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Base de donnnées, filesystem |
| Données de sorties | Aucune |
| Données sources archivées | Non |
| Structure des données sources | X |
| Structure des données de sortie | X |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | X |
| Connexions | db_data_store |

<br />

Pour plus d'informations, rendez-vous [ici](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo)
