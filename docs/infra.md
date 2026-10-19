# Documentation du dossier `infra`

Le dossier `infra` contient les abstractions et wrappers pour toutes les interactions avec l'infrastructure externe : bases de données, système de fichiers, clients HTTP, envoi d'emails, catalogues de données, Grist, et Airflow. Cette couche permet de découpler la logique métier des détails techniques d'implémentation.

## Vue d'ensemble

```
infra/
├── airflow/            # Tâches Airflow génériques (dag, task, common_tasks)
├── catalog/            # Gestion des catalogues de données (Iceberg, Polaris)
├── database/           # Intéragir avec les bases de données (PostgreSQL, SQLite, Trino)
├── file_system/        # Intéragir avec les gestionnaires de fichiers (local, S3)
├── grist/              # Client spécialisé pour Grist
├── http_client/        # Intéragir avec les serveurs webs (HTTP, API, ...)
├── mails/              # Système d'envoi d'emails
└── yaml/               # Lecture des contextes de datasets au format YAML
```

## 1. Module Catalog (`infra/catalog/`)

### Description
Gestion des catalogues de données pour les tables Iceberg et l'administration avec Polaris.

### Classes principales

#### `IcebergCatalog` (Dataclass)
Client pour interagir avec un catalogue Iceberg :
- `create_namespace()` : Création de namespaces hiérarchiques
- `create_table()` : Création de table à partir d'un schéma DataFrame
- `update_table()` : Mise à jour du schéma avec de nouvelles colonnes
- `write_table()` : Écriture dans une table Iceberg à partir d'un DataFrame
- `write_table_and_namespace()` : Création du namespace et écriture
- `read_table()` : Chargement d'une table depuis le catalogue
- `read_table_as_df()` : Lecture d'une table sous forme de DataFrame pandas
- `drop_table()` : Suppression de table (avec purge optionnelle des données)
- `list_tables()` : Liste des tables dans un namespace (avec filtrage par pattern)

#### `PolarisCatalog` (Dataclass)
Client pour l'administration d'un catalogue Polaris :
- `get_root_access_token()` : Obtention d'un token OAuth
- `get_catalog_info()` : Récupération des métadonnées du catalogue
- `create_catalog()` : Création d'un catalogue avec configuration S3
- `delete_catalog()` : Suppression d'un catalogue
- `create_principal()` : Création d'un principal et récupération des credentials
- `create_principal_role()`, `create_catalog_role()` : Gestion des rôles
- `assign_principal_role()`, `assign_catalog_role()` : Attribution des rôles
- `grant_catalog_privileges()` : Attribution de privilèges de gestion

### Initialisation

```python
from modules.infra.catalog.iceberg import IcebergCatalog, generate_catalog_properties

properties = generate_catalog_properties(
    uri="https://polaris-catalog.lab.incubateur.finances.rie.gouv.fr",
    warehouse="cat_data_store",
)

catalog = IcebergCatalog(
    name="my_catalog",
    properties=properties,
)

# Écriture d'un DataFrame (staging)
from modules.infra.catalog.iceberg import IcebergTableStatus
catalog.write_table_and_namespace(
    df=my_dataframe,
    table_status=IcebergTableStatus.STAGING,
    namespace="my_namespace",
    table_name="my_table",
)

# Lecture
df = catalog.read_table_as_df(table_name="my_namespace.my_table")
```

### Fonctions utilitaires

```python
from modules.infra.catalog.iceberg import generate_catalog_properties

# Génération des propriétés de catalogue (SSL, credentials, options custom)
properties = generate_catalog_properties(
    uri="https://polaris-catalog.lab.incubateur.finances.rie.gouv.fr",
    warehouse="cat_data_store",
    client_id="root",          # optionnel, lu depuis Airflow Variable si absent
    client_secret="root",      # optionnel, lu depuis Airflow Variable si absent
    ca_bundle_path=None,       # optionnel, chemin vers un bundle CA
    options=None,              # options supplémentaires passées au catalogue
)
```

## 2. Module Database (`infra/database/`)

### Description
Fournit une interface unifiée pour les opérations de base de données.
Bases de données supportées :
- PostgreSQL
- SQLite
- Trino (lecture seule)

### Classes principales

#### `DBInterface` (Classe abstraite)
Interface de base définissant les méthodes communes :
- `get_uri()` : Récupération de l'URI de connexion
- `get_conn()` : Obtention de la connexion à la base
- `execute()` : Exécution de requêtes sans retour
- `fetch_one()`, `fetch_all()` : Récupération de lignes sous forme de dictionnaires
- `fetch_df()` : Récupération sous forme de DataFrame pandas
- `fetch_table_columns()` : Récupération des colonnes d'une table
- `fetch_table_pk()` : Récupération des clés primaires d'une table
- `insert()`, `bulk_insert()` : Insertion de données
- `update()` : Mise à jour de lignes
- `delete()` : Suppression de lignes
- `begin()`, `commit()`, `rollback()` : Gestion des transactions
- `copy_expert()` : Opération COPY en masse (type PostgreSQL)

#### `PgAdapter`
Implémentation pour PostgreSQL. Supporte deux modes d'initialisation : via un Airflow connection ID ou des paramètres psycopg2 directs.

#### `SQLiteAdapter`
Implémentation pour SQLite via sqlite3.

#### `TrinoAdapter`
Implémentation en lecture seule pour Trino. Les opérations d'écriture (`insert`, `bulk_insert`, `update`, `delete`) lèvent `NotImplementedError`.

### Repositories

- `DbDatasetContextRepository` : Implémentation de `DatasetContextRepository` via la base de données
- `DbProjetRepository` : Implémentation de `ProjetRepository` via la base de données

### Initialisation

```python
from modules.infra.database.factory import create_db_handler, DatabaseType, DbConfig

# PostgreSQL (par défaut)
db = create_db_handler(
    db_type=DatabaseType.POSTGRES,
    db_config=DbConfig(connection_id="db_data_store"),
)

# SQLite
db = create_db_handler(
    db_type=DatabaseType.SQLITE,
    db_config=DbConfig(db_path="/path/to/database.db"),
)

# Trino (lecture seule)
db = create_db_handler(
    db_type=DatabaseType.TRINO,
    db_config=DbConfig(
        host="trino.example.com",
        user="my_user",
        catalog="my_catalog",
        port=443,
        schema="my_schema",
    ),
)

# Utilisation
results = db.fetch_all("SELECT * FROM my_table WHERE id = %s", (123,))
df = db.fetch_df("SELECT * FROM my_table")
```

## 3. Module File System (`infra/file_system/`)

### Description
Système unifié de gestion des fichiers supportant différents backends de stockage (local, S3/MinIO).

### Classes principales

#### `FSInterface` (Classe abstraite)
Interface de base pour les opérations sur fichiers :
- `read()` : Lire un fichier
- `write()` : Écrire un fichier
- `delete()` : Supprimer des fichiers
- `delete_single()` : Supprimer un fichier unique
- `exists()` : Vérifier la présence d'un fichier
- `get_metadata()` : Récupérer des métadonnées (`FileMetadata`)
- `list_files()` : Lister des fichiers (avec patterns glob)
- `move()` : Déplacer un fichier
- `copy()` : Copier un fichier
- `validate()` : Valider un fichier selon des critères
- `get_absolute_path()` : Convertir un chemin relatif → absolu

#### `LocalFS`
Gestion des fichiers sur le système de fichiers local. Supporte les écritures atomiques via fichiers temporaires.

#### `S3FS`
Gestion des fichiers sur S3/MinIO utilisant Airflow S3Hook ou un client boto3. Supporte plusieurs formats (CSV, Excel, Parquet, JSON), l'inférence de content-type et le listage paginé.

### Dataset Location

Permet de résoudre l'emplacement physique d'un dataset (S3, local, DB, Grist) à partir de son type logique :

- `S3DatasetLocationProvider` : Lecture/écriture via S3
- `LocalFileDatasetLocationProvider` : Lecture/écriture sur le système local
- `DbDatasetLocationProvider` : Lecture/écriture via une base de données
- `GristDatasetLocationProvider` : Lecture depuis Grist

`DatasetLocationFactory` : fabrique qui instancie le bon provider selon le `DatasetLocation`.

### Initialisation

```python
from modules.infra.file_system.factory import create_file_handler, FileHandlerType, FSConfig

# Fichiers locaux
local_handler = create_file_handler(
    handler_type=FileHandlerType.LOCAL,
    config=FSConfig(base_path="/tmp"),
)

# S3/MinIO
s3_handler = create_file_handler(
    handler_type=FileHandlerType.S3,
    config=FSConfig(bucket="my-bucket", connection_id="minio_conn"),
)

# Utilisation
content = local_handler.read("path/to/file.txt")
s3_handler.write("remote/key/file.txt", "Hello World")
```

### Utilitaires DataFrame

```python
from modules.infra.file_system.dataframe import read_dataframe, write_dataframe

# Lecture d'un DataFrame depuis différents formats
df = read_dataframe(
    file_handler=s3_handler,
    file_path="data.csv",
    read_options={"sep": ";", "encoding": "utf-8"},
)

# Écriture d'un DataFrame
write_dataframe(
    df=my_dataframe,
    file_handler=s3_handler,
    file_path="output/parquet/file.parquet",
)
```

### Sérialisation de données

Le module `data_serializer.py` fournit des sérialiseurs pour convertir entre DataFrame et bytes :
- `CSVSerializer`
- `ParquetSerializer`
- `ExcelSerializer`
- `JSONSerializer`

## 4. Module HTTP Client (`infra/http_client/`)

### Description
Clients HTTP génériques avec gestion des erreurs et configuration avancée.

### Classes principales

#### `HttpInterface` (Classe abstraite)
Interface de base pour les clients HTTP (supporte le context manager) :
- `request()` : Requête HTTP générique
- `get()`, `post()`, `put()`, `patch()`, `delete()` : Méthodes HTTP spécifiques
- `close()` : Libération des ressources

#### `HTTPResponse`
Wrapper pour les réponses HTTP :
- `json()` : Récupération de la réponse en JSON
- `text` : Propriété pour récupérer la réponse en texte
- `content` : Contenu brut de la réponse
- `headers` : Headers de la réponse
- `status_code` : Code de statut HTTP
- `raise_for_status()` : Lève une exception si le statut est en erreur

#### `ClientConfig` (Dataclass)
Configuration du client HTTP :
- `base_url` : URL de base
- `timeout` : Timeout en secondes (défaut: 30)
- `verify_ssl` : Vérification SSL (défaut: True)
- `auth_token` : Token d'authentification
- `auth_type` : Type d'authentification ("Bearer", "Basic")
- `default_headers` : Headers par défaut
- `user_agent` : User-Agent personnalisé
- `proxy_trust_env` : Utiliser le proxy de l'environnement
- `proxy` : Proxy explicite
- `proxy_auth` : Authentification proxy
- `max_retries` : Nombre maximum de retries (défaut: 3)
- `retry_statuses` : Statuts HTTP qui déclenchent un retry
- `retry_methods` : Méthodes HTTP éligibles au retry
- `rate_limit` : Limitation de débit (requêtes/seconde)

#### Adaptateurs
- `RequestsClient` : Implémentation basée sur le package `requests`
- `HttpxClient` : Implémentation basée sur `httpx`

### Initialisation

```python
from modules.infra.http_client.factory import create_http_client, HttpHandlerType
from modules.infra.http_client.config import ClientConfig

config = ClientConfig(
    timeout=30,
    auth_token="your_token",
    auth_type="Bearer",
)

# Requests
client = create_http_client(client_type=HttpHandlerType.REQUEST, config=config)

# HTTPx
client = create_http_client(client_type=HttpHandlerType.HTTPX, config=config)

# Utilisation
response = client.get("https://api.example.com/users")
if response.status_code == 200:
    users = response.json()
```

## 5. Module Grist (`infra/grist/`)

### Description
Client spécialisé pour interagir avec l'API Grist.

### Classe principale

#### `GristClient`
Client pour les opérations Grist. Méthodes principales utilisées dans les pipelines :

- `download_doc(doc_id)` : Télécharger un document Grist (format zip)
- `list_records(doc_id, table_id)` : Lister les enregistrements d'une table
- `add_records(doc_id, table_id, json, batch_size)` : Ajouter des enregistrements
- `get_df_from_records(doc_id, table_id)` : Récupérer les enregistrements sous forme de DataFrame
- `send_dataframe_to_grist(df, doc_id, table_id, ...)` : Envoyer un DataFrame vers Grist
- `list_tables(doc_id)` : Lister les tables d'un document
- `list_orgs()` : Lister les organisations

### Initialisation

```python
from modules.infra.grist.client import GristClient
from modules.infra.http_client.factory import create_http_client, HttpHandlerType
from modules.infra.http_client.config import ClientConfig

config = ClientConfig(timeout=30)
http_client = create_http_client(client_type=HttpHandlerType.REQUEST, config=config)

grist = GristClient(
    http_client=http_client,
    grist_host="https://grist.numerique.gouv.fr",
    api_token="your_token",
)

# Utilisation
doc_id = "your_doc_id"
df = grist.get_df_from_records(doc_id=doc_id, table_id="TableName")
grist.send_dataframe_to_grist(df=my_dataframe, doc_id=doc_id, table_id="TableName")
```

## 6. Module Mails (`infra/mails/`)

### Description
Système d'envoi d'emails avec support des templates Jinja2 et intégration Airflow SMTP.

### Classes principales

#### `MailMessage` (Dataclass)
Structure d'un message email :
- `to` : Destinataires principaux (obligatoire)
- `mail_status` : Statut optionnel (`MailStatus`) pour génération automatique du template et du sujet
- `subject` : Sujet (auto-généré si `mail_status` est fourni)
- `html_content` : Contenu HTML (auto-généré si `mail_status` est fourni)
- `template_dir` : Répertoire personnalisé de templates
- `template_parameters` : Paramètres pour le rendu du template
- `cc`, `bcc` : Destinataires en copie
- `from_email` : Expéditeur
- `files` : Pièces jointes
- `custom_headers` : En-têtes personnalisés

Si `mail_status` est fourni, le template et le sujet sont automatiquement générés. Sinon, `html_content` et `subject` doivent être fournis manuellement.

#### `MailStatus` (StrEnum)
- `START`, `SUCCESS`, `ERROR`, `SKIP`, `WARNING`, `INFO`

#### `MailPriority` (Enum)
- `NORMAL`, `LOW`, `HIGH`

### Fonctions principales

#### `send_mail()`
Envoi d'un email via Airflow SMTP. Valide que le message possède un sujet et un contenu HTML.

#### `render_template()`
Rendu d'un template Jinja2 avec paramètres.

#### `create_send_mail_callback()`
Crée une fonction callback Airflow pour les notifications automatiques de pipeline.

### Initialisation

```python
from modules.infra.mails.default_smtp import MailMessage, send_mail, create_send_mail_callback, MailStatus

# Envoi avec statut (template auto-généré)
message = MailMessage(
    to=["user@example.com"],
    mail_status=MailStatus.SUCCESS,
    template_parameters={"dag_id": "my_dag", "execution_date": "2025-01-01"},
)
send_mail(message)

# Envoi avec contenu personnalisé
message = MailMessage(
    to=["user@example.com"],
    subject="Notification",
    html_content="<p>Mon contenu HTML</p>",
)
send_mail(message)
```

### Templates intégrés
- `pipeline_start.html` : Début de pipeline
- `pipeline_end_success.html` : Succès de pipeline
- `pipeline_end_error.html` : Erreur de pipeline
- `logs_recap.html` : Récapitulatif des logs

### Callbacks Airflow

```python
from modules.infra.mails.default_smtp import create_send_mail_callback, MailStatus

# Création de callbacks pour notifications automatiques
on_failure = create_send_mail_callback(MailStatus.ERROR)
on_success = create_send_mail_callback(MailStatus.SUCCESS)

# Utilisation dans DAG
@dag(
    on_failure_callback=on_failure,
    on_success_callback=on_success,
)
def my_dag():
    pass
```

## 7. Module Airflow (`infra/airflow/`)

### Description
Tâches Airflow génériques réutilisables dans tous les pipelines.

### Fichiers

| Fichier | Contenu |
|---------|---------|
| `dag.py` | `create_dag_params`, `create_default_args`, `AirflowDagRepository`, `should_skip_task` |
| `task.py` | `create_task` — fabrique de tâches Airflow génériques basées sur `PipelineDescriptor` |
| `common_tasks/validation.py` | `validate_dag_parameters` |
| `common_tasks/sql.py` | `create_projet_snapshot`, `update_projet_snapshot_status`, `ensure_partition`, `create_tmp_tables`, `delete_tmp_tables`, `copy_tmp_table_to_real_table`, `refresh_views` |
| `common_tasks/s3.py` | `copy_s3_files`, `del_s3_files`, `del_iceberg_staging_table`, `copy_staging_to_prod` |
| `common_tasks/grist.py` | `download_grist_doc_to_s3`, `generic_grist_processing` |
| `common_tasks/projet.py` | `get_documentation_task`, `get_contact_task`, `get_source_fichier_task`, `get_projet_datasets_context`, `config_projet_group` |
| `common_tasks/dbt/tasks.py` | `run_dbt_build`, `dbt_build_models` |

### `create_task`

Fabrique une tâche Airflow à partir d'un `PipelineDescriptor`. C'est le cœur du framework :

```python
from modules.infra.airflow.task import create_task
from modules.domain.pipeline.model import PipelineDescriptor
from modules.domain.dataset.model import Dataset

pipeline = PipelineDescriptor(
    input_datasets=(Dataset("ma_table_source"),),
    output_dataset=Dataset("ma_table_traitée"),
    operation=ma_fonction_processing,
)

mon_task = create_task(
    pipeline=pipeline,
    execution_options=execution_options,  # dict[str, ExecutionOptions] du config.py
)
```

### `create_dag_params` / `create_default_args`

```python
from modules.infra.airflow.dag import create_dag_params, create_default_args

params = create_dag_params(
    nom_projet="Mon Projet",
    dag_status=DagStatus.RUN,
    db_params=DBParams(prod_schema="mon_schema"),
    feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=False, convert_files=False, download_grist_doc=False),
)

default_args = create_default_args(retries=0)
```

## 8. Module YAML (`infra/yaml/`)

### Description
Lecture des contextes de datasets au format YAML. Utile pour les DAGs de vérification ou les configurations locales.

### Classes principales

#### `YamlDatasetContextRepository` (Dataclass)
Implémentation de `DatasetContextRepository` qui lit la configuration depuis un fichier YAML :
- `get_list(nom_projet)` : Liste des datasets du projet
- `get(nom_projet, nom_dataset)` : Récupération d'un dataset spécifique
- `get_list_source_fichier(nom_projet)` : Liste des fichiers sources
- `get_list_column_mapping(nom_projet, dataset_name)` : Mapping des colonnes

### Initialisation

```python
from modules.infra.yaml.dataset_context import YamlDatasetContextRepository

repo = YamlDatasetContextRepository(yaml_path="path/to/dataset_context.yaml")
datasets = repo.get_list(nom_projet="Mon Projet")
```

## Bonnes pratiques

### 1. Utilisation des factory fonctions
Toujours utiliser les factory fonctions (`create_db_handler`, `create_file_handler`, `create_http_client`) pour créer les instances.

### 2. Gestion des erreurs
Les modules lèvent des exceptions standard Python (`ValueError`, `NotImplementedError`, `FileNotFoundError`, `RuntimeError`). Capturer les exceptions spécifiques au contexte plutôt que de swallower silencieusement.

### 3. Configuration centralisée
Utiliser les classes de configuration (`DbConfig`, `FSConfig`, `ClientConfig`) pour centraliser les paramètres.

### 4. Lazy loading
Les handlers utilisent l'initialisation paresseuse des connexions pour optimiser les performances.

### 5. Context managers
Utiliser les context managers quand disponibles pour la gestion automatique des ressources (ex. `HttpInterface`).

## Tests et mocking

Chaque handler peut être mocké facilement grâce aux interfaces abstraites :

```python
from unittest.mock import Mock
from modules.infra.database.base import DBInterface

# Mock pour les tests
mock_db = Mock(spec=DBInterface)
mock_db.fetch_all.return_value = [{"id": 1, "name": "test"}]
```

Cette architecture modulaire facilite les tests unitaires et l'évolution du code en isolant les dépendances externes.
