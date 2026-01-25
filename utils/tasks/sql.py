"""SQL task utilities using infrastructure handlers."""

import logging
import textwrap
from typing import Optional
from datetime import datetime, timedelta

import psycopg2
from airflow.sdk import task
from airflow.sdk import get_current_context

from infra.database.base import BaseDBHandler
from infra.database.factory import create_db_handler

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.factory import create_default_s3_handler, create_local_handler

from utils.config.dag_params import (
    get_dag_status,
    get_db_info,
    get_execution_date,
    get_project_name,
)
from utils.config.tasks import get_projet_db_info, get_tbl_names
from enums.database import (
    LoadStrategy,
    PartitionTimePeriod,
)
from types.dags import DagStatus
from types.projet import DbInfo, SelecteurInfo
from utils.control.structures import are_lists_egal
from utils.config.vars import (
    DEFAULT_TMP_SCHEMA,
    DEFAULT_PG_DATA_CONN_ID,
    DEFAULT_PG_CONFIG_CONN_ID,
    DEFAULT_S3_CONN_ID,
)

# ------------------------------------------------------------------------------
# Internal functions
# ------------------------------------------------------------------------------


def _get_primary_keys(schema: str, table: str, db_handler: BaseDBHandler) -> list[str]:
    """Get primary key columns of a table."""
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
                AND tc.constraint_schema = kcu.constraint_schema
        WHERE tc.table_schema = %s
            AND tc.table_name = %s
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position;
    """
    df = db_handler.fetch_df(query, parameters=(schema, table))
    return df.loc[:, "column_name"].tolist()


def _create_snapshot_id(
    nom_projet: str, execution_date: datetime, db_handler: BaseDBHandler
) -> None:

    snapshot_id = execution_date.strftime(format="%Y%m%d_%H:%M:%S")
    query = """
        INSERT INTO conf_projets.projet_snapshot (id_projet, snapshot_id, creation_timestamp)
        SELECT
            p.id,
            %(snapshot_id)s,
            %(creation_timestamp)s
        FROM conf_projets.projet p
        WHERE p.nom_projet = %(nom_projet)s
        AND EXISTS (SELECT 1 FROM conf_projets.projet WHERE nom_projet = %(nom_projet)s);
    """

    # Paramètres pour la requête
    params = {
        "nom_projet": nom_projet,
        "snapshot_id": snapshot_id,
        "creation_timestamp": execution_date.replace(tzinfo=None),
    }

    # Exécution de la requête
    db_handler.execute(query, parameters=params)


def _get_snapshot_id(nom_projet: str, db_handler: BaseDBHandler) -> str:
    query = """
        WITH cte_id_projet AS (
            SELECT id as id_projet
            FROM conf_projets.projet
            WHERE nom_projet = %(nom_projet)s
        ),
        ranked_snapshots AS (
            SELECT psi.snapshot_id,
                    ROW_NUMBER() OVER (ORDER BY psi.creation_timestamp DESC) as rank
            FROM conf_projets.projet_snapshot psi
            WHERE psi.id_projet = (SELECT id_projet FROM cte_id_projet)
        )
        SELECT snapshot_id
        FROM ranked_snapshots
        WHERE rank = 1;
    """

    # Paramètres pour la requête
    params = {"nom_projet": nom_projet}

    # Exécution de la requête
    db_result = db_handler.fetch_one(query, parameters=params)

    if db_result is None:
        raise ValueError(f"No db_result found for project {nom_projet}")

    snapshot_id = db_result.get("snapshot_id", None)

    if snapshot_id is None:
        raise ValueError(f"No snapshot_id found for project {nom_projet}")

    return snapshot_id


def determine_partition_period(
    time_period: PartitionTimePeriod, execution_date: datetime
) -> tuple[datetime, datetime]:
    """Determine the start and end dates for a partition based on the time period."""
    if time_period == PartitionTimePeriod.YEAR:
        from_date_period = execution_date.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period.replace(year=from_date_period.year + 1)
    elif time_period == PartitionTimePeriod.MONTH:
        from_date_period = execution_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        if from_date_period.month == 12:
            to_date_period = from_date_period.replace(
                year=from_date_period.year + 1, month=1
            )
        else:
            to_date_period = from_date_period.replace(month=from_date_period.month + 1)
    elif time_period == PartitionTimePeriod.WEEK:
        from_date_period = execution_date - timedelta(days=execution_date.weekday())
        from_date_period = from_date_period.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period + timedelta(weeks=1)
    elif time_period == PartitionTimePeriod.DAY:
        from_date_period = execution_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period + timedelta(days=1)
    else:
        raise ValueError(f"Unsupported time period: {time_period}")
    return (from_date_period, to_date_period)


# ------------------------------------------------------------------------------
# SQL tasks
# ------------------------------------------------------------------------------


@task
def create_projet_snapshot(
    pg_conn_id: str = DEFAULT_PG_CONFIG_CONN_ID, **context
) -> None:
    """ """
    nom_projet = get_project_name(context=context)
    execution_date = get_execution_date(context=context)
    dag_status = get_dag_status(context=context)

    if dag_status == DagStatus.DEV:
        print(f"Pipeline start execution date: {execution_date}")
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    # Hook
    db_handler = create_db_handler(connection_id=pg_conn_id)

    _create_snapshot_id(
        nom_projet=nom_projet, execution_date=execution_date, db_handler=db_handler
    )


@task
def get_projet_snapshot(
    nom_projet: Optional[str] = None,
    pg_conn_id: str = DEFAULT_PG_CONFIG_CONN_ID,
    **context,
) -> None:
    """
    Récupérer le dernier snapshot_id d'un projet.

    Args:
        nom_projet (optionnel): Le nom du projet. A spécifier lorsque le nom du projet
            dans le DAG est différent de celui qui génère le snapshot_id,
        pg_conn_id: Connexion Postgres. Valeur par défaut

    Returns:
        None. Ajoute le snapshot_id dans le context du DAG
    """
    if nom_projet is None:
        nom_projet = get_project_name(context=context)

    # Hook
    db_handler = create_db_handler(connection_id=pg_conn_id)

    snapshot_id = _get_snapshot_id(nom_projet=nom_projet, db_handler=db_handler)
    print(f"Adding snapshot_id {snapshot_id} to context")
    context["ti"].xcom_push(key="snapshot_id", value=snapshot_id)
    print("Snapshot_id added to context.")


@task
def ensure_partition(
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Vérifie si une partition mensuelle existe pour une table partitionnée par date.
    Si elle n'existe pas, la crée.

    Args:
        pg_conn_id: Connexion Postgres
        partition_column: Colonne de partition (par défaut 'import_date')

    Returns:
        Le nom de la partition (créée ou existante)
    """
    if nom_projet is None:
        nom_projet = get_project_name(context=context)
    dag_status = get_dag_status(context=context)
    execution_date = get_execution_date(context=context)
    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    # Récupérer les informations de la table parente
    tbl_info = get_projet_db_info(nom_projet=nom_projet)

    db = create_db_handler(connection_id=pg_conn_id)

    for tbl in tbl_info:
        tbl_name = tbl.tbl_name

        if not tbl.is_partitionned:
            print(f"{tbl_name} is not partitinned ... skipping")
            continue

        # Get partition period range
        from_date, to_date = determine_partition_period(
            time_period=PartitionTimePeriod(value=tbl.partition_period.upper()),
            execution_date=execution_date,
        )

        # Nom de la partition : parenttable_YYYY_MM
        partition_name = f"{tbl_name}_{from_date.strftime(format='%Y%m%d')}_{to_date.strftime(format='%Y%m%d')}"  # noqa

        try:
            logging.info(msg=f"Creating partition {partition_name} for {tbl_name}.")
            # Créer la partition
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {prod_schema}.{partition_name}
                PARTITION OF {prod_schema}.{tbl_name}
                FOR VALUES FROM ('{from_date}') TO ('{to_date}');
            """
            db.execute(query=create_sql)
            logging.info(msg=f"Partition {partition_name} created successfully.")
        except psycopg2.errors.DuplicateTable:
            logging.info(
                msg=f"Partition {partition_name} already exists. Skipping creation."
            )
        except Exception as e:
            logging.error(msg=f"Error creating partition {partition_name}: {str(e)}")
            raise


@task(task_id="create_tmp_tables")
def create_tmp_tables(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    reset_id_seq: bool = True,
    **context,
) -> None:
    """
    Used to create temporary tables in the database.
    """
    nom_projet = get_project_name(context=context)
    dag_status = get_dag_status(context=context)

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema

    # Hook
    db = create_db_handler(connection_id=pg_conn_id)

    tbl_names = get_tbl_names(nom_projet=nom_projet)

    rows_result = db.fetch_all(
        query="""SELECT COUNT(*) as count_tmp_tables
            FROM information_schema.tables
            WHERE table_schema='temporaire'
                AND table_name LIKE 'tmp_%';
            """
    )

    drop_queries = []
    create_queries = []
    alter_queries = []

    for table in tbl_names:
        drop_queries.append(f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{table};")
        create_queries.append(
            f"""CREATE TABLE
                IF NOT EXISTS {tmp_schema}.tmp_{table}
                ( LIKE {prod_schema}.{table} INCLUDING ALL);
            """
        )
        alter_queries.append(
            f"ALTER SEQUENCE {prod_schema}.{table}_id_seq RESTART WITH 1;"
        )

    if rows_result[0]["count_tmp_tables"] != 0:
        for drop_query in drop_queries:
            db.execute(query=drop_query)

    for create_query in create_queries:
        db.execute(query=create_query)
    if reset_id_seq:
        for alter_query in alter_queries:
            db.execute(query=alter_query)


@task(task_id="delete_tmp_tables")
def delete_tmp_tables(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Used to delete temporary tables in the database.
    """
    nom_projet = get_project_name(context=context)

    db_info = get_db_info(context=context)
    tmp_schema = db_info.tmp_schema

    tbl_info = get_projet_db_info(nom_projet=nom_projet)
    # Hook
    db = create_db_handler(connection_id=pg_conn_id)

    for tbl in tbl_info:
        tbl_name = tbl.tbl_name
        db.execute(query=f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{tbl_name};")


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    projet_db_info: list[DbInfo] | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Permet de copier les tables temporaires dans les tables réelles.

    strategy:
        FULL_LOAD      -> delete all prod rows, insert everything from tmp
        INCREMENTAL    -> UPSERT + delete missing rows based on primary key
        APPEND    -> ADD all rows from tmp to prod
    """
    nom_projet = get_project_name(context=context)
    dag_status = get_dag_status(context=context)

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema

    # Hook
    db_handler = create_db_handler(connection_id=pg_conn_id)

    if projet_db_info is None:
        projet_db_info = get_projet_db_info(nom_projet=nom_projet)
    print(f"Nombre de tables à copier: {len(projet_db_info)}")

    queries = []
    for db_info in projet_db_info:
        load_strategy = LoadStrategy(value=db_info.load_strategy.upper())
        tbl_name = db_info.tbl_name
        prod_table = f"{prod_schema}.{tbl_name}"
        tmp_table = f"{tmp_schema}.tmp_{tbl_name}"

        if load_strategy == LoadStrategy.APPEND:
            query = f"INSERT INTO {prod_table} SELECT * FROM {tmp_table};"
            queries.append(query)

        if load_strategy == LoadStrategy.FULL_LOAD:
            del_query = f"DELETE FROM {prod_table};"
            insert_query = f"INSERT INTO {prod_table} SELECT * FROM {tmp_table};"
            queries.append(del_query)
            queries.append(insert_query)

        if load_strategy == LoadStrategy.INCREMENTAL:
            pk_cols = _get_primary_keys(
                schema=prod_schema, table=tbl_name, db_handler=db_handler
            )
            col_list = sort_db_colnames(tbl_name=tbl_name, pg_conn_id=pg_conn_id)

            merge_query = f"""
                MERGE INTO {prod_table} tbl_target
                USING {tmp_table} tbl_source ON ({' AND '.join([f'tbl_source.{col} = tbl_target.{col}' for col in pk_cols])})
                WHEN MATCHED THEN
                    UPDATE SET {", ".join([f"{col}=tbl_source.{col}" for col in col_list if col not in pk_cols])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(col_list)})
                        VALUES ({', '.join([f'tbl_source.{col}' for col in col_list])})
                /* Only for PG v17+
                WHEN NOT MATCHED BY SOURCE THEN
                    DELETE;
                */
            """
            queries.append(merge_query)

        if queries:
            for q in queries:
                db_handler.execute(query=q)
        else:
            print("No query to execute")


def sort_db_colnames(
    tbl_name: str,
    keep_file_id_col: bool = False,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> list[str]:
    """Get sorted column names from a table.

    Args:
        tbl_name: Table name
        keep_file_id_col: Whether to include id column
        schema: Schema name

    Returns:
        Sorted list of column names
    """
    db = create_db_handler(connection_id=pg_conn_id)
    df = db.fetch_df(
        query="""SELECT isc.table_catalog, isc.table_schema, isc.table_name, isc.column_name
            FROM information_schema.columns isc
            WHERE
                isc.table_schema = %s
                AND isc.table_name = %s
                AND (isc.column_default NOT LIKE 'nextval%%' OR isc.column_default IS NULL)
            ORDER BY table_schema ASC, table_name ASC, column_name ASC;
        """,
        parameters=(schema, tbl_name),
    )

    cols = df.loc[:, "column_name"].tolist()
    if keep_file_id_col:
        logging.info(msg="Parameter keep_file_id_col is deprecated")

    sorted_cols = sorted(cols)
    logging.info(msg=f"Sorted columns for table {tbl_name}: {sorted_cols}")
    return sorted_cols


def bulk_load_local_tsv_file_to_db(
    local_filepath: str,
    tbl_name: str,
    column_names: list[str],
    db_handler: BaseDBHandler,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> None:
    """Bulk load TSV file into database using COPY.

    Args:
        local_filepath: Path to local TSV file
        tbl_name: Target table name
        column_names: List of column names in order
        schema: Target schema
    """
    logging.info(f"Bulk importing {local_filepath} to {schema}.tmp_{tbl_name}")

    copy_sql = f"""
        COPY {schema}.tmp_{tbl_name} ({', '.join(column_names)})
        FROM STDIN WITH (
            FORMAT TEXT,
            DELIMITER E'\t',
            HEADER TRUE,
            NULL 'NULL'
        )
    """

    db_handler.copy_expert(sql=copy_sql, filepath=local_filepath)
    logging.info(
        msg=f"Successfully loaded {local_filepath} into {schema}.tmp_{tbl_name}"
    )


@task(map_index_template="{{ import_task_name }}")
def import_file_to_db(
    selecteur_info: SelecteurInfo,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    keep_file_id_col: bool = False,
    use_prod_schema: bool = True,
    **context,
) -> None:
    selecteur = selecteur_info.selecteur
    db_info = get_db_info(context=context)
    dag_status = get_dag_status(context=context)
    context = get_current_context()
    context["import_task_name"] = selecteur  # type: ignore

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    if use_prod_schema:
        schema = db_info.prod_schema
    else:
        schema = db_info.tmp_schema

    # Variables
    local_filepath = "/tmp/" + selecteur_info.filename
    s3_filepath = selecteur_info.filepath_tmp_s3
    tbl_name = selecteur_info.tbl_name

    if tbl_name is None or tbl_name == "":
        print(f"tbl_name is None for selecteur <{selecteur}>. Nothing to import to db")
    else:
        # Hooks
        db_handler = create_db_handler(connection_id=pg_conn_id)
        s3_handler = create_default_s3_handler(connection_id=s3_conn_id)
        local_handler = create_local_handler(base_path=None)

        # Check if old file exists
        local_handler.delete(file_path=local_filepath)

        # Read data from s3, sort its columns and save it locally
        print(f"Reading file from remote < {s3_filepath} >")
        df = read_dataframe(file_handler=s3_handler, file_path=s3_filepath)

        sorted_df_cols = sorted(df.columns)
        df = df.reindex(labels=sorted_df_cols, axis=1).convert_dtypes()
        print(f"DF : {sorted_df_cols}")
        print(f"Saving file to local < {local_filepath} >")
        local_handler.write(
            file_path=local_filepath,
            content=df.to_csv(index=False, sep="\t", na_rep="NULL"),
        )

        # Check if columns are the same between df and db table
        sorted_db_colnames = sort_db_colnames(
            tbl_name=tbl_name,
            keep_file_id_col=keep_file_id_col,
            pg_conn_id=pg_conn_id,
            schema=schema,
        )
        if not are_lists_egal(list_A=sorted_df_cols, list_B=sorted_db_colnames):
            raise ValueError(
                textwrap.dedent(
                    text="""
                Il y a des différences entre les colonnes du DataFrame et de la Table.
                Impossible d'importer les données.
            """
                )
            )

        # Bulk load file to db
        bulk_load_local_tsv_file_to_db(
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            column_names=sorted_db_colnames,
            db_handler=db_handler,
        )

        # Clean up local file
        local_handler.delete(file_path=local_filepath)


@task(task_id="set_dataset_last_update")
def set_dataset_last_update_date(
    dataset_ids: list[int], pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context
) -> None:
    db = create_db_handler(connection_id=pg_conn_id)
    # Vérifier que le dataset existe
    datasets = db.fetch_df(
        query="""
            SELECT id
            FROM documentation.datasets
            WHERE id = ANY(%s);""",
        parameters=(dataset_ids,),
    )

    if len(datasets) == 0:
        raise ValueError("Aucune dataset ne correspond aux ids fournis")

    if len(datasets) < len(dataset_ids):
        raise ValueError("Certains datasets n'ont pas été trouvés.")

    # ne devrait jamais arriver
    if len(datasets) > len(dataset_ids):
        raise ValueError("Certains datasets possèdent le même id.")

    # update la date de dernière mise à jour
    execution_date = context.get("execution_date")
    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")
    for dataset_id in dataset_ids:
        db.execute(
            query="""UPDATE documentation.datasets
                    SET last_update = %s
                    WHERE id=%s;
            """,
            parameters=(execution_date, dataset_id),
        )


@task
def refresh_views(pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context) -> None:
    """Tâche pour actualiser les vues matérialisées"""
    dag_status = get_dag_status(context=context)
    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    db = create_db_handler(connection_id=pg_conn_id)

    get_mview_query = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = %s;
    """

    views = db.fetch_df(query=get_mview_query, parameters=(prod_schema,))[
        "matviewname"
    ].tolist()

    if len(views) == 0:
        print(f"No materialized views found for schema {prod_schema}. Skipping ...")
    else:
        sql_queries = [
            f"REFRESH MATERIALIZED VIEW {prod_schema}.{view_name};"
            for view_name in views
        ]
        for query in sql_queries:
            db.execute(query=query)
