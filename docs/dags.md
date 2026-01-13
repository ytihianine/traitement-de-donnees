# Guide de création des pipelines (dags)

Ce guide explique comment créer des pipelines Airflow (appelées DAGs dans Airflow) en utilisant les tâches pré-définies disponibles dans le dossier `utils/` et/ou en créant ses propres tâches.

## Table des matières

1. [Architecture et Principes](#architecture-et-principes)
2. [Structure des Paramètres](#structure-des-paramètres)
3. [Tâches Pré-définies Disponibles](#tâches-pré-définies-disponibles)
4. [Créer ses Fonctions de Processing](#créer-ses-fonctions-de-processing)
5. [Exemple Complet de DAG](#exemple-complet-de-dag)
6. [Bonnes Pratiques](#bonnes-pratiques)
7. [Gestion des Erreurs](#gestion-des-erreurs)

## Architecture et Principes

### Principe de Séparation des Responsabilités

Le framework propose une architecture en couches :

- **DAGs (`dags/`)** : Orchestration des traitements métiers
- **Infrastructure (`infra/`)** : Interaction avec les systèmes externes (base de données, S3, HTTP, mails)
- **Enums (`enums/`)** : Enums transverses nécessaires dans les dags, tâches, fonctions ...
- **Entities (`entities/`)** : Types transverses nécessaires dans les dags, tâches, fonctions ...
- **Utilitaires (`utils/`)** : Tâches réutilisables et configuration

Les dags doivent respecter [cette organisation](./convention.md#dags)

### Workflow Standard

Il existe deux worflows principaux génériques qui nécessitent d'être adapté à chaque pipeline.
Le premier workflow permet d'effectuer un ETL classique. Il contient les étapes suivantes
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Extraction** : Lecture des données depuis diverses sources (S3, Grist, base de données)
3. **Transformation** : Application de fonctions de processing personnalisées
4. **Chargement** : Sauvegarde des résultats (S3, base de données)
5. **Notification** : Envoi de mails de succès/échec

Le second workflow permet des actions qui ne nécessitent pas nécessairement de données. Il contient les étapes suivantes
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Actions**: Réalise une action définie (ping, envoi de mail, requête API ...)
5. **Notification** : Envoi de mails de succès/échec


Les workflows peuvent être plus complexes et mélanger des étapes de chacun de ces workflows. Les étapes à absolument conserver sont
- **Validation des paramètres**
- **Notification**

## Structure des Paramètres

Chaque DAG doit définir ses paramètres selon la structure TypedDict suivante :

```python
from airflow.sdk import dag
from entities.dags import DagParams, DagStatus

@dag(
    dag_id="id_unique_du_dag",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["Tag1", "Tag2"],
    description="Traitement des données comptables issues de Chorus",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet="Mon Projet",
        dag_status=DagStatus.DEV,
        prod_schema="production",
        mail_enable=False,
        mail_to=None,
        mail_cc=None,
        lien_pipeline="https://...",
        lien_donnees="https://...",
    ),
    # Autres arguments
)
```

## Tâches Pré-définies Disponibles

### 1. Validation des Paramètres

```python
from utils.tasks.validation import create_validate_params_task

# Création de la tâche de validation
validate_params = create_validate_params_task(
    required_paths=[
        "nom_projet",
        "dag_status",
        "db.prod_schema",
        "db.tmp_schema",
        "mail.to"
    ],
)
```

### 2. Tâches ETL (Extract, Transform, Load)

#### ETL depuis Grist
```python
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.etl import create_grist_etl_task

# Télécharger le document Grist
grist_doc = download_grist_doc_to_s3(
    selecteur="grist_doc,
    workspace_id="grist_ws_id",
    doc_id_key="grist_doc_ic,
    grist_host=DEFAULT_GRIST_HOST,
    api_token_key="grist_secret_key",
    use_proxy=True,
)

# ETL Grist pour traiter une table du document avec fonction de processing personnalisée
grist_etl = create_grist_etl_task(
    selecteur="mon_selecteur",
    doc_selecteur="grist_doc",
    normalisation_process_func=ma_fonction_normalisation,
    process_func=ma_fonction_processing
)
```

#### ETL Générique
```python
from utils.tasks.etl import create_task
from entities.dags import TaskConfig, ETLStep

# ETL générique avec traitement personnalisé
etl_task = create_task(
    task_config=TaskConfig(
        task_id="my_task_id"
    ),
    output_selecteur="selecteur",
    steps=[
        ETLStep(
            fn=ma_fonction_processing,
            kwargs={"additional_fn_args": True} # kwargs passés à la function
            use_context=True,
            read_data=True
        ),
        ...
        ETLStep(
            fn=ma_fonction_processing_2,
            use_previous_output=True
        ),
    ],
    input_selecteurs=["input_1", "input_2"],
    add_import_date=True,
    add_snapshot_id=True,
    export_output=True,
)
```

### 3. Gestion des Fichiers

#### Conversion vers Parquet
```python
from utils.tasks.file import create_parquet_converter_task

# Conversion de fichiers vers Parquet
convert_to_parquet = create_parquet_converter_task(
    selecteur="mon_selecteur",
    process_func=ma_fonction_processing,
    read_options={"encoding": "utf-8", "sep": ";"},
    apply_cols_mapping=True
)
```

### 4. Opérations SQL

#### Création de Tables Temporaires
```python
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    ensure_partition,
    import_file_to_db,
    LoadStrategy,
)
from utils.config.vars import (
    DEFAULT_PG_DATA_CONN_ID,
    DEFAULT_S3_CONN_ID,
)

# Création des tables temporaires
create_tables = create_tmp_tables()

# Importer les données -- Tâche dynamique
import_task = import_file_to_db.partial(
        pg_conn_id=DEFAULT_PG_DATA_CONN_ID,
        s3_conn_id=DEFAULT_S3_CONN_ID,
        keep_file_id_col=True,
        use_prod_schema=True
    ).expand(
        selecteur_config=get_projet_config(nom_projet=nom_projet)
    )

# Création de partition mensuelle
create_partition = ensure_partition()

# Copie des données vers production
copy_to_prod = copy_tmp_table_to_real_table(
    load_strategy=LoadStrategy.FULL_LOAD,
)
copy_to_prod = copy_tmp_table_to_real_table(
    load_strategy=LoadStrategy.INCREMENTAL,
)
copy_to_prod = copy_tmp_table_to_real_table(
    load_strategy=LoadStrategy.APPEND,
)
```

### 5. Opérations S3

```python
from utils.tasks.s3 import copy_s3_files, del_s3_files

# Copie de fichiers S3
copy_files = copy_s3_files(bucket="mon-bucket")

# Suppression de fichiers S3
delete_files = del_s3_files(bucket="mon-bucket")
```

## Bonnes Pratiques

### 1. Naming et Organisation

```python
# ✅ Bon : Utilisation de préfixes clairs
@dag("pipeline_ventes_mensuelles", ...)
def pipeline_ventes_mensuelles():
    extract_data = create_grist_etl_task(...)
    transform_data = create_parquet_converter_task(...)

# ❌ Éviter : Noms génériques
@dag("dag1", ...)
def my_dag():
    task1 = create_grist_etl_task(...)
```

### 2. Paramétrage

```python
# ✅ Bon : Utilisation des constantes
from utils.config.vars import (
    DEFAULT_S3_BUCKET, DEFAULT_PG_DATA_CONN_ID
)

# ✅ Bon : Validation systématique
validate_params = create_validate_params_task(
    required_paths=["nom_projet", "db.prod_schema"],
    task_id="validate_params"
)
```

### 3. Gestion des Dépendances

```python
# ✅ Bon : Utilisation de chain pour la lisibilité
chain(
    validate_params(),
    extract_data(),
    [transform_data(), compute_metrics()],  # Parallélisation
    load_data(),
    cleanup()
)
```

### 4. Documentation

```python
# ✅ Bon : Documentation des fonctions
def calculer_taux_conversion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le taux de conversion par canal marketing.

    Args:
        df: DataFrame contenant les données de marketing
        taux: float.

    Returns:
        DataFrame avec les taux de conversion calculés

    Notes: (Optionnel)
        taux: s'exprime entre 0 et 1

    Logique métier: (Optionnel)
        - Taux = (nb_conversions / nb_visiteurs) * 100
        - Filtrage des canaux avec moins de 100 visiteurs
    """
    # Implementation...
```

## Gestion des Erreurs

### 1. Callbacks de Notification

```python
from infra.mails.default_smtp import create_airflow_callback, MailStatus

@dag(
    ...,
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS)
)
def mon_dag():
    # Tasks avec callbacks individuels
    risky_task = create_etl_task(
        selecteur="data_source",
        on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR)
    )
```

### 2. Retry et Timeout

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Task avec timeout spécifique
detect_files = S3KeySensor(
    ...,
    timeout=timedelta(hours=2),
    poke_interval=timedelta(minutes=5),
    soft_fail=True  # Continue même en cas d'échec
)
```

---

Ce guide vous permet de créer des DAGs robustes en utilisant les tâches pré-définies et vos propres fonctions de processing métier. Pour plus d'informations, consultez la [documentation de l'infrastructure](infra.md) et les [conventions du projet](convention.md).
