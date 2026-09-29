"""SQL task utilities using infrastructure handlers."""

import logging
from collections.abc import Mapping

from airflow.sdk import task

from modules.constants import (
    DEFAULT_PG_DATA_CONN_ID,
    DEFAULT_TMP_SCHEMA,
)
from modules.containers import DEFAULT_DAG_REPO, DEFAULT_DATASET_CONTEXT_REPO, DEFAULT_PROJET_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.model import TypeLocation
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.pipeline.model import (
    ExecutionOptions,
    LoadStrategy,
    determine_partition_period,
)
from modules.domain.projet.repository import ProjetRepository
from modules.infra.airflow.dag import (
    AirflowDagRepository,
    should_skip_task,
)
from modules.infra.database.base import DBInterface
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataset_location import parse_db_schema, parse_db_table


# ------------------------------------------------------------------------------
# SQL tasks
# ------------------------------------------------------------------------------
@task
def create_projet_snapshot(
    nom_projet: str | None = None,
    nom_projet_parent: str | None = None,
    dag_repo: AirflowDagRepository = DEFAULT_DAG_REPO,
    projet_repo: ProjetRepository = DEFAULT_PROJET_REPO,
    **context,
) -> None:
    """ """
    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)
    execution_date = dag_repo.get_execution_date(context=context)

    projet_repo.create_projet_metadata(
        nom_projet=nom_projet, execution_date=execution_date, nom_projet_parent=nom_projet_parent
    )


@task
def update_projet_snapshot_status(
    nom_projet: str | None = None,
    status: bool = True,
    dag_repo: AirflowDagRepository = DEFAULT_DAG_REPO,
    projet_repo: ProjetRepository = DEFAULT_PROJET_REPO,
    **context,
) -> None:
    """
    Lorsque le DAG est complété, mettre à jour le statut du snapshot_id du projet à la valeur de `status`
    dans la table versioning.snapshot.

    Args:
        nom_projet (optionnel): Le nom du projet. A spécifier lorsque le nom du projet
            dans le DAG est différent de celui qui génère le snapshot_id,
        dag_repo: Instance du repository pour accéder aux informations du DAG. Par défaut, utilise DEFAULT_DAG_REPO.
        pg_conn_id: Connexion Postgres. Valeur par défaut utilise DEFAULT_PG_DATA_CONN_ID.
        projet_repo: Instance du repository pour accéder aux informations du projet. Par défaut, utilise DEFAULT_PROJET_REPO.

    Returns:
        None.
    """

    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)

    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    # Update is_dag_completed to True for the snapshot_id
    projet_repo.update_projet_metadata_status(nom_projet=nom_projet, status=status)
    logging.info(msg=f"Updated latest snapshot_id. Set is_dag_completed to {status}")


@task
def ensure_partition(
    execution_options: Mapping[str, ExecutionOptions],
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    dag_repo: AirflowDagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context,
) -> None:
    """
    Vérifie si une partition mensuelle existe pour une table partitionnée par date.
    Si elle n'existe pas, la créer.

    Args:
        dataset_context: Instance du dataset context pour lequel vérifier/créer la partition
        execution_options: Mapping des options d'exécution
        pg_conn_id: Connexion Postgres
        partition_column: Colonne de partition (par défaut 'import_date')

    Returns:
        Le nom de la partition (créée ou existante)
    """
    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)

    execution_date = dag_repo.get_execution_date(context=context)

    # Init vars
    db = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    )

    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)

    for index, dataset_context in enumerate(datasets_context):
        logging.info(msg=f"{index + 1}/{len(datasets_context)} Processing dataset {dataset_context.dataset_name}")

        dataset_options = execution_options.get(dataset_context.dataset_name)
        if dataset_options is None:
            raise ValueError("No execution options found for dataset.")

        if not dataset_options.is_partitioned:
            logging.info(msg=f"{dataset_context.dataset_name} is not partitioned ... skipping")
            continue

        dest_loc = dataset_context.dest_loc
        if dest_loc.type_location != TypeLocation.DB:
            logging.warning(msg="Destination location type not database ... skipping partition creation")
            continue

        if dest_loc.validate_conn_id != pg_conn_id:
            db = create_db_handler(
                db_type=DatabaseType.POSTGRES,
                db_config=DbConfig(connection_id=dest_loc.validate_conn_id),
            )

        schema = parse_db_schema(dest_loc.validate_location)
        tbl_name = parse_db_table(dest_loc.validate_location)
        partition_period = dataset_options.partition_period

        # Get partition period range
        from_date, to_date = determine_partition_period(
            time_period=partition_period,
            execution_date=execution_date,
        )

        # Nom de la partition : parenttable_YYYY_MM
        partition_name = f"{tbl_name}_{from_date.strftime(format='%Y%m%d')}_{to_date.strftime(format='%Y%m%d')}"

        try:
            logging.info(msg=f"Creating partition {partition_name} for {tbl_name}.")
            # Créer la partition
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
                PARTITION OF {schema}.{tbl_name}
                FOR VALUES FROM
                    ('{from_date.strftime(format="%Y-%m-%d")}') TO ('{to_date.strftime(format="%Y-%m-%d")}');
            """
            db.execute(query=create_sql)
            logging.info(msg=f"Partition {partition_name} created successfully.")
        except Exception as e:
            logging.error(msg=f"Error creating partition {partition_name}: {e!s}")
            raise


@task(task_id="create_tmp_tables")
def create_tmp_tables(
    execution_options: Mapping[str, ExecutionOptions],
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    reset_id_seq: bool = False,
    dag_repo: AirflowDagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context,
) -> None:
    """
    Used to create temporary tables in the database.
    """

    if should_skip_task(context=context):
        return

    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)

    db_info = dag_repo.get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema
    logging.info(msg=f"Prod schema: {prod_schema}, Tmp schema: {tmp_schema}")

    # Init vars
    db = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    )
    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)

    drop_queries = []
    create_queries = []
    alter_queries = []

    for index, dataset_context in enumerate(datasets_context):
        logging.info(msg=f"{index + 1}/{len(datasets_context)} Processing dataset {dataset_context.dataset_name}")

        tmp_loc = dataset_context.tmp_loc
        if tmp_loc.type_location != TypeLocation.DB:
            logging.info(msg=f"Skipping DB tmp table creation for selecteur <{dataset_context.dataset_name}>")
            continue

        tbl_name = parse_db_table(location=tmp_loc.validate_location)

        drop_queries.append(f"DROP TABLE IF EXISTS {tmp_schema}.{tbl_name};")
        create_queries.append(f"""CREATE TABLE
                IF NOT EXISTS {tmp_schema}.{tbl_name}
                ( LIKE {prod_schema}.{tbl_name} INCLUDING ALL);
            """)
        alter_queries.append(f"ALTER SEQUENCE {tmp_schema}.{tbl_name}_id_seq RESTART WITH 1;")

    for drop_query in drop_queries:
        db.execute(query=drop_query)

    for create_query in create_queries:
        db.execute(query=create_query)
    if reset_id_seq:
        for alter_query in alter_queries:
            db.execute(query=alter_query)


@task(task_id="delete_tmp_tables")
def delete_tmp_tables(
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    dag_repo: AirflowDagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context,
) -> None:
    """
    Used to delete temporary tables in the database.
    """
    # Init vars
    db = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    )

    db_info = dag_repo.get_db_info(context=context)
    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)
    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)

    for dataset_context in datasets_context:
        tmp_loc = dataset_context.tmp_loc
        if tmp_loc.type_location != TypeLocation.DB:
            logging.warning(
                msg=f"Temporary location for dataset {dataset_context.dataset_name} is not a DB table ... skipping"
            )
            continue

        tbl_name = parse_db_table(location=tmp_loc.validate_location)
        db.execute(query=f"DROP TABLE IF EXISTS {db_info.tmp_schema}.tmp_{tbl_name};")


def _create_append_copy_query(prod_table: str, tmp_table: str, col_list: list[str]) -> str:
    """Create SQL query for APPEND load strategy."""
    cols = ", ".join(col_list)
    return f"INSERT INTO {prod_table} ({cols}) SELECT {cols} FROM {tmp_table};"


def _create_full_load_copy_query(prod_table: str, tmp_table: str, col_list: list[str]) -> str:
    """Create SQL query for FULL_LOAD load strategy."""
    cols = ", ".join(col_list)
    return f"DELETE FROM {prod_table}; INSERT INTO {prod_table} ({cols}) SELECT {cols} FROM {tmp_table};"


def _create_incremental_copy_query(
    prod_table: str,
    tmp_table: str,
    col_list: list[str],
    pk_cols: list[str],
    merge_delete: bool = False,
) -> str:
    """Create SQL query for INCREMENTAL load strategy."""
    merge_query = f"""
        MERGE INTO {prod_table} tbl_target
        USING {tmp_table} tbl_source ON ({' AND '.join([f'tbl_source.{col} = tbl_target.{col}' for col in pk_cols])})
        WHEN MATCHED THEN
            UPDATE SET {", ".join([f"{col}=tbl_source.{col}" for col in col_list if col not in pk_cols])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(col_list)})
                VALUES ({', '.join([f'tbl_source.{col}' for col in col_list])})
    """

    if merge_delete:
        merge_query += """
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE
            ;
        """

    return merge_query


def _generate_copy_query(
    tbl_name: str,
    load_strategy: LoadStrategy,
    db_handler: DBInterface,
    prod_schema: str = DEFAULT_TMP_SCHEMA,
    tmp_schema: str = DEFAULT_TMP_SCHEMA,
    merge_delete: bool = False,
) -> str:
    """
    Generate SQL query to copy data from temporary table to production table based on the specified strategy.

    Args:
        tbl_name: Name of the table to copy.
        db_handler: Database handler object.
        prod_schema: Production schema name.
        tmp_schema: Temporary schema name.
        merge_delete: Whether to perform merge delete operation.
        load_strategy: Load strategy to use for copying data.
    """
    prod_table = f"{prod_schema}.{tbl_name}"
    tmp_table = f"{tmp_schema}.tmp_{tbl_name}"

    col_list = db_handler.fetch_table_columns(
        schema=prod_schema,
        table=tbl_name,
    )

    def _build_incremental_query() -> str:
        pk_cols = db_handler.fetch_table_pk(schema=prod_schema, table=tbl_name)
        logging.info(msg=f"Table <{tbl_name}> primary key: {pk_cols}")
        return _create_incremental_copy_query(
            prod_table=prod_table,
            tmp_table=tmp_table,
            col_list=col_list,
            pk_cols=pk_cols,
            merge_delete=merge_delete,
        )

    _registry = {
        LoadStrategy.APPEND: lambda: _create_append_copy_query(
            prod_table=prod_table,
            tmp_table=tmp_table,
            col_list=col_list,
        ),
        LoadStrategy.FULL_LOAD: lambda: _create_full_load_copy_query(
            prod_table=prod_table,
            tmp_table=tmp_table,
            col_list=col_list,
        ),
        LoadStrategy.INCREMENTAL: _build_incremental_query,
    }

    return _registry[load_strategy]()


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    execution_options: Mapping[str, ExecutionOptions],
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    merge_delete: bool = False,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context,
) -> None:
    """
    Permet de copier les tables temporaires dans les tables réelles.

    strategy:
        FULL_LOAD      -> delete all prod rows, insert everything from tmp
        INCREMENTAL    -> UPSERT + delete missing rows based on primary key
        APPEND    -> ADD all rows from tmp to prod
    """
    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    if nom_projet is None:
        nom_projet = dag_repo.get_project_name(context=context)
    db_info = dag_repo.get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema

    # Hook
    db_handler = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    )
    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)

    # Sort by tbl_order to handle foreign key dependencies
    logging.info(msg=f"Nombre de tables à copier: {len(datasets_context)}")

    queries = []
    for dataset_context in datasets_context:
        tmp_loc = dataset_context.tmp_loc
        if tmp_loc.type_location != TypeLocation.DB:
            logging.warning(
                msg=f"Temporary location for dataset {dataset_context.dataset_name} is not a DB table ... skipping"
            )
            continue

        dest_loc = dataset_context.dest_loc
        if dest_loc.type_location != TypeLocation.DB:
            logging.warning(
                msg=f"Destination location for dataset {dataset_context.dataset_name} is not a DB table ... skipping"
            )
            continue

        dataset_exec_options = execution_options.get(dataset_context.dataset_name)
        if dataset_exec_options is None:
            raise ValueError(f"No execution options found for dataset {dataset_context.dataset_name}")

        queries.append(
            _generate_copy_query(
                tbl_name=parse_db_table(location=dest_loc.validate_location),
                load_strategy=dataset_exec_options.load_strategy,
                db_handler=db_handler,
                prod_schema=prod_schema,
                tmp_schema=tmp_schema,
                merge_delete=merge_delete,
            )
        )

    if len(queries) == 0:
        logging.info(msg="No query to execute")
        return

    for q in queries:
        try:
            db_handler.execute(query=q)
        except Exception as e:
            logging.error(msg=f"Failed to execute query: {q}")
            raise e


def bulk_load_local_tsv_file_to_db(
    local_filepath: str,
    tbl_name: str,
    column_names: list[str],
    db_handler: DBInterface,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> None:
    """Bulk load TSV file into database using COPY.

    Args:
        local_filepath: Path to local TSV file
        tbl_name: Target table name
        column_names: List of column names in order
        schema: Target schema
    """
    logging.info(msg=f"Bulk importing {local_filepath} to {schema}.tmp_{tbl_name}")

    copy_sql = f"""
        COPY {schema}.tmp_{tbl_name} ({", ".join(column_names)})
        FROM STDIN WITH (
            FORMAT TEXT,
            DELIMITER E'\t',
            HEADER TRUE,
            NULL 'NULL'
        )
    """

    db_handler.copy_expert(
        sql=copy_sql,
        filepath=local_filepath,
    )
    logging.info(msg=f"Successfully loaded {local_filepath} into {schema}.tmp_{tbl_name}")


@task
def refresh_views(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, dag_repo: DagRepository = DEFAULT_DAG_REPO, **context
) -> None:
    """Tâche pour actualiser les vues matérialisées"""
    if should_skip_task(context=context):
        return

    db_info = dag_repo.get_db_info(context=context)
    prod_schema = db_info.prod_schema

    db = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    )

    get_mview_query = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = %s;
    """

    views = db.fetch_df(query=get_mview_query, parameters=(prod_schema,))["matviewname"].tolist()

    if len(views) == 0:
        logging.info(msg=f"No materialized views found for schema {prod_schema}. Skipping ...")
    else:
        sql_queries = [f"REFRESH MATERIALIZED VIEW {prod_schema}.{view_name};" for view_name in views]
        for query in sql_queries:
            db.execute(query=query)
