# Guide de création des pipelines (dags)

Ce guide explique comment créer des pipelines Airflow (appelées DAGs dans Airflow) en utilisant les tâches pré-définies disponibles dans `modules/` et/ou en créant ses propres fonctions de processing.

## Table des matières

1. [Architecture et Principes](#architecture-et-principes)
2. [Structure des Paramètres](#structure-des-paramètres)
3. [Créer une Tâche Pipeline](#créer-une-tâche-pipeline)
4. [Tâches Pré-définies Disponibles](#tâches-pré-définies-disponibles)
5. [Exemple Complet de DAG](#exemple-complet-de-dag)
6. [Bonnes Pratiques](#bonnes-pratiques)
7. [Gestion des Erreurs](#gestion-des-erreurs)

## Architecture et Principes

### Principe de Séparation des Responsabilités

Le framework propose une architecture en couches :

- **DAGs (`dags/`)** : Orchestration des traitements métiers
- **Domain (`modules/domain/`)** : Modèles métier (Datasets, DAGs, Projets, Pipeline)
- **Infrastructure (`modules/infra/`)** : Interaction avec les systèmes externes (base de données, S3, HTTP, mails, catalogues)
- **Traitement générique (`modules/generic_processing/`)** : Fonctions de traitement réutilisables (dates, textes, structures, nombres)
- **Constantes (`modules/constants.py`)** : Variables communes à toutes les pipelines

Les dags doivent respecter [cette organisation](./convention.md#dags)

### Workflow Standard

Il existe deux workflows principaux génériques qui nécessitent d'être adaptés à chaque pipeline.
Le premier workflow permet de réaliser un ETL classique. Il contient les étapes suivantes :
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Extraction** : Lecture des données depuis diverses sources (S3, Grist, base de données, Iceberg)
3. **Transformation** : Application de fonctions de processing personnalisées via un `PipelineDescriptor`
4. **Chargement** : Sauvegarde des résultats (S3, base de données, Iceberg)
5. **Notification** : Envoi de mails de succès/échec

Le second workflow permet de réaliser des actions qui ne nécessitent pas nécessairement de données. Il contient les étapes suivantes :
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Actions** : Réalise une action définie (ping, envoi de mail, requête API ...)
3. **Notification** : Envoi de mails de succès/échec

Les workflows peuvent être plus complexes et mélanger des étapes de chacun de ces workflows. Les étapes à absolument conserver sont :
- **Validation des paramètres**
- **Notification**

## Structure des Paramètres

Chaque DAG doit définir ses paramètres selon la structure suivante :

```python
from airflow.sdk import dag
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.mails.default_smtp import create_send_mail_callback, MailStatus
from modules.infra.airflow.dag import create_dag_params, create_default_args

@dag(
    dag_id="id_unique_du_dag",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["Tag1", "Tag2"],
    description="Description courte",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(
            prod_schema="schema_prod",
            tmp_schema="schema_tmp",
        ),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=False,
            s3=False,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
```

Les `FeatureFlagsEnable` permettent d'activer/désactiver certaines fonctionnalités du dag et/ou des tâches sans avoir à modifier le code.

## Créer une Tâche Pipeline

Le framework utilise le pattern `PipelineDescriptor` pour définir les tâches génériques. Chaque pipeline est décrit par :
- Des datasets d'entrée (sources de données)
- Un dataset de sortie (destination)
- Une fonction de transformation à appliquer

### Structure d'une Fonction de Processing

```python
import pandas as pd

def ma_fonction_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fonction de processing personnalisée.

    Args:
        df: DataFrame source (ou df_input_1, df_input_2 si multiples inputs)

    Returns:
        DataFrame transformé
    """
    # Transformation...
    return df
```

### Création d'un PipelineDescriptor

```python
from modules.domain.pipeline.model import PipelineDescriptor
from modules.domain.dataset.model import Dataset

# Pipeline avec un seul input
pipeline = PipelineDescriptor(
    input_datasets=(Dataset(name="ma_table_source"),),
    output_dataset=Dataset(name="ma_table_traitée"),
    operation=ma_fonction_processing,
    add_metadata=True,  # Ajoute import_timestamp et snapshot_id
)

# Pipeline avec plusieurs inputs
pipeline_multi = PipelineDescriptor(
    input_datasets=(
        Dataset(name="table_1"),
        Dataset(name="table_2"),
    ),
    output_dataset=Dataset(name="table_fusionnée"),
    operation=lambda df_table_1, df_table_2: df_table_1.merge(df_table_2),
    add_metadata=False,
)
```

### Configuration des options d'exécution

Les `ExecutionOptions` sont définies dans le `config.py` de chaque DAG sous forme d'un dictionnaire clé-valeur, où la clé est le nom du dataset :

```python
# config.py
from modules.domain.pipeline.model import ExecutionOptions, PartitionTimePeriod

execution_options = {
    "ma_table_source": ExecutionOptions(
        tbl_order=1,
        read_options={"encoding": "utf-8", "sep": ";"},
        is_partitioned=True,
        partition_period=PartitionTimePeriod.MONTH,
        load_strategy=LoadStrategy.INCREMENTAL,
    ),
    "ma_table_traitée": ExecutionOptions(tbl_order=5),
}
```

### Création de la Tâche Airflow

```python
from modules.infra.airflow.task import create_task

# Créer la tâche (retourne un callable Airflow)
mon_task = create_task(
    pipeline=pipeline,
    execution_options=execution_options,
)
```

### Exécution de la Tâche dans un DAG

```python
from airflow.sdk.bases.operator import chain

with mon_dag():
    validate = validate_dag_parameters()
    result = mon_task()  # Appel du task
    chain(
        validate,
        result,
    )
```

## Tâches Pré-définies Disponibles

### 1. Validation des Paramètres

```python
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
```

### 2. Tâches de Processing Grist

#### Télécharger un document Grist vers S3

```python
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3

# Télécharger le document Grist en début de DAG
grist_doc = download_grist_doc_to_s3(
    dataset_name="grist_doc",
    use_proxy=True,
)
```

#### Processing générique de tables Grist

```python
from modules.infra.airflow.common_tasks.grist import generic_grist_processing

# Fonction de processing Grist intégrée
def ma_table_processing(df):
    return generic_grist_processing(
        df=df,
        cols_to_keep=["colonne_1", "colonne_2"],
        cols_mapping={"colonne_source": "colonne_cible"},
        txt_columns=["colonne_2"],
        date_columns=["colonne_date"],
        num_columns=["colonne_nombre"],
        bool_columns=["colonne_bool"],
        ref_columns=["colonne_ref"],
        custom_fn=ma_fonction_processing,  # Fonction de processing métier optionnelle
    )
```

### 3. Opérations SQL

```python
from modules.infra.airflow.common_tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    create_projet_snapshot,
    ensure_partition,
    update_projet_snapshot_status,
)
from dags.mon_projet.config import execution_options

# Création du snapshot du projet
create_snapshot = create_projet_snapshot()

# Création des tables temporaires
create_tables = create_tmp_tables(execution_options=execution_options, reset_id_seq=False)

# Création des partitions
create_partition = ensure_partition(execution_options=execution_options)

# Copie des données vers production
copy_to_prod = copy_tmp_table_to_real_table(execution_options=execution_options)

# Suppression des tables temporaires
delete_tables = delete_tmp_tables(execution_options=execution_options)

# Mettre à jour le statut du snapshot à la fin
update_status = update_projet_snapshot_status()
```

#### Stratégies de chargement SQL

Les stratégies de chargement sont définies dans `ExecutionOptions.load_strategy` :
- `LoadStrategy.APPEND` : Ajoute toutes les lignes de la table temporaire à la table de production
- `LoadStrategy.FULL_LOAD` : Supprime toutes les lignes de production, insère tout depuis la table temporaire
- `LoadStrategy.INCREMENTAL` : UPSERT basé sur les clés primaires (avec option `merge_delete`)

### 4. Opérations S3 / Iceberg

```python
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
    copy_staging_to_prod,
    del_iceberg_staging_table,
)

# Suppression des tables Iceberg de staging (avant traitement)
del_staging = del_iceberg_staging_table()

# Copie de fichiers S3 (tmp -> prod)
copy_files = copy_s3_files(execution_options=execution_options)

# Suppression de fichiers S3
delete_files = del_s3_files(execution_options=execution_options)

# Copie tables Iceberg de staging vers production (tâche dynamique, expand)
datasets_context = get_projet_datasets_context()
copy_staging = copy_staging_to_prod.expand(dataset_context=datasets_context)
```

### 5. Opérations de Projet

```python
from modules.infra.airflow.common_tasks.projet import (
    get_documentation_task,
    get_contact_task,
    get_source_fichier_task,
    get_projet_datasets_context,
    config_projet_group,
)

# Tâches individuelles
docs = get_documentation_task()
contacts = get_contact_task()
sources = get_source_fichier_task()
datasets_ctx = get_projet_datasets_context()

# Ou utiliser le groupe de tâches complet
projet_config = config_projet_group()
```

## Exemple Complet de DAG

Voici un exemple complet basé sur le pattern réel utilisé dans le projet (cf. `dags/sg/dsci/demo/`) :

### `config.py`

```python
from modules.domain.pipeline.model import ExecutionOptions

nom_projet = "Démo traitement données"

execution_options = {
    "ref_direction": ExecutionOptions(tbl_order=1),
    "ref_intervention": ExecutionOptions(tbl_order=1),
    "accompagnement": ExecutionOptions(tbl_order=5),
}
```

### `process.py`

```python
import pandas as pd

def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    return df

def process_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    # Transformation métier...
    return df
```

### `tasks.py`

```python
from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
            **grist_kwargs,
        ),
    )


@task_group
def referentiels() -> None:
    ref_direction = create_task(
        pipeline=_grist_pipeline(
            "ref_direction",
            process.process_ref_direction,
            txt_columns=["direction"],
        ),
        execution_options=config.execution_options,
    )
    chain([ref_direction()])


@task_group
def activite() -> None:
    accompagnement = create_task(
        pipeline=_grist_pipeline(
            "accompagnement",
            process.process_accompagnement,
            cols_to_keep=["id", "date_de_la_demande", "direction"],
            date_columns=["date_de_la_demande"],
            txt_columns=["direction"],
        ),
        execution_options=config.execution_options,
    )
    chain([accompagnement()])
```

### `dags.py`

```python
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo.config import execution_options, nom_projet
from dags.sg.dsci.demo.tasks import activite, referentiels
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3
from modules.infra.airflow.common_tasks.s3 import copy_s3_files, del_s3_files
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    ensure_partition,
    update_projet_snapshot_status,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback


@dag(
    dag_id="demo_traitement_donnees",
    schedule="0 8-12,14-18 * * 1-5",
    default_args=create_default_args(),
    max_consecutive_failed_dag_runs=1,
    max_active_runs=1,
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="demo_traitement"),
        feature_flags=FeatureFlagsEnable(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def demo_traitement_dag() -> None:
    chain(
        validate_dag_parameters(),
        download_grist_doc_to_s3(dataset_name="grist_doc", workspace_id="dsci"),
        create_projet_snapshot(),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        [referentiels(), activite()],
        ensure_partition(execution_options=execution_options),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(execution_options=execution_options),
        del_s3_files(execution_options=execution_options),
        delete_tmp_tables(execution_options=execution_options),
        update_projet_snapshot_status(),
    )


demo_traitement_dag()
```

## Bonnes Pratiques

### 1. Naming et Organisation

```python
# Bon : Utilisation de préfixes clairs
@dag("pipeline_ventes_mensuelles", ...)
def pipeline_ventes_mensuelles():
    extract_data = create_task(...)

# Éviter : Noms génériques
@dag("dag1", ...)
def my_dag():
    task1 = create_task(...)
```

### 2. Paramétrage

```python
# Bon : Utilisation des constantes
from modules.constants import (
    DEFAULT_S3_BUCKET, DEFAULT_PG_DATA_CONN_ID, DEFAULT_S3_CONN_ID,
    DEFAULT_GRIST_HOST, DEFAULT_POLARIS_HOST,
)
```

### 3. Gestion des Dépendances

```python
# Bon : Utilisation de chain pour la lisibilité
chain(
    validate_dag_parameters(),
    grist_doc(),
    [transform_data(), compute_metrics()],  # Parallélisation
    load_data(),
    cleanup()
)
```

### 4. Documentation

```python
# Bon : Documentation des fonctions
def calculer_taux_conversion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le taux de conversion par canal marketing.

    Args:
        df: DataFrame contenant les données de marketing

    Returns:
        DataFrame avec les taux de conversion calculés

    Notes: (Optionnel)
        Taux s'exprime entre 0 et 1

    Logique métier: (Optionnel)
        - Taux = (nb_conversions / nb_visiteurs) * 100
        - Filtrage des canaux avec moins de 100 visiteurs
    """
    # Implémentation...
```

## Gestion des Erreurs

### 1. Callbacks de Notification

```python
from modules.infra.mails.default_smtp import create_send_mail_callback, MailStatus

@dag(
    ...,
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def mon_dag():
    ...
```

### 2. Retry et Timeout

```python
from modules.infra.airflow.dag import create_default_args

# Retry au niveau du DAG
default_args = create_default_args(
    retries=2,
    retry_delay=timedelta(minutes=5),
)

# Retry au niveau d'une tâche (via le décorateur @task)
@task(
    task_id="ma_tache_avec_retry",
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
)
def ma_tache_avec_retry():
    ...
```

---

Ce guide vous permet de créer des DAGs robustes en utilisant les tâches pré-définies et vos propres fonctions de processing métier. Pour plus d'informations, consultez la [documentation de l'infrastructure](infra.md) et les [conventions du projet](convention.md).
