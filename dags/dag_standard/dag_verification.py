from pprint import pprint

from airflow.sdk import Variable, dag, get_current_context, task
from airflow.sdk.bases.operator import chain
from dags.dag_standard.config import execution_options
from modules.constants import (
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_HOST,
    DEFAULT_S3_CONN_ID,
    DEFAULT_TRINO_HOST,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.domain.dataset.model import DatasetContext
from modules.infra.airflow.common_tasks.projet import (
    config_projet_group,
    get_projet_datasets_context,
    get_source_fichier_task,
)
from modules.infra.airflow.common_tasks.s3 import del_iceberg_staging_table
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import (
    AirflowDagRepository,
    create_dag_params,
    create_default_args,
)
from modules.infra.catalog.iceberg import (
    IcebergCatalog,
    IcebergTableStatus,
    generate_catalog_properties,
)
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler
from modules.infra.mails.default_smtp import (
    MailMessage,
    MailStatus,
    _callback,
    send_mail,
)

nom_projet = "Configuration des projets"


# Définition du DAG
@dag(
    dag_id="dag_verification",
    schedule=None,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "Vérification"],
    description="Dag de vérification.",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="iceberg"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=True,
            download_grist_doc=True,
        ),
    ),
)
def dag_verification() -> None:
    @task
    def print_context(**context) -> None:
        pprint(object=context)
        pprint(object=context["dag"].__dict__)
        pprint(object=context["ti"].__dict__)
        pprint(object=context["ti"].xcom_pull(key="snapshot_id", task_ids="get_projet_snapshot"))

    @task
    def send_simple_mail(**context) -> None:
        mail_message = MailMessage(
            to=["yanis.tihianine@finances.gouv.fr"],
            cc=["yanis.tihianine@finances.gouv.fr"],
            subject="Simple test depuis Airflow",
            html_content="Test réussi !",
        )
        send_mail(mail_message=mail_message)

    @task
    def send_error_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.ERROR)

    @task
    def send_success_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.SUCCESS)

    @task
    def check_s3_hook() -> None:
        s3_hook = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(connection_id=DEFAULT_S3_CONN_ID),
        )
        keys = s3_hook.list_files(directory="data_store/test_namespace/")
        print(keys)
        print(len(keys))
        keys_with_pattern = s3_hook.list_files(directory="data_store/test_namespace/", pattern="*_staging*")
        print(keys_with_pattern)
        print(len(keys_with_pattern))

        s3_hook.delete_single(file_path="data_store/test_namespace/test_table_staging/")

    @task
    def check_trino_hook() -> None:
        from modules.infra.database.trino import TrinoAdapter

        trino_user = Variable.get(key="TRINO_USER")

        trino_handler = TrinoAdapter(
            host=DEFAULT_TRINO_HOST,
            user=trino_user,
            catalog=DEFAULT_POLARIS_CATALOG,
            port=443,
            http_scheme="https",
            verify=False,
        )
        df = trino_handler.fetch_df(query='SELECT * FROM "infrastructure.configuration.projet".direction')
        print(df.head())

    @task
    def iceberg_task(**context) -> None:
        import pandas as pd

        properties = generate_catalog_properties(
            uri=DEFAULT_POLARIS_HOST,
        )
        catalog = IcebergCatalog(name="data_store", properties=properties)

        df = pd.DataFrame(data={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        db_schema = AirflowDagRepository().get_db_info(context=context).prod_schema
        key = "key/sub_key/my_table_test"
        key_split = key.split("/")
        tbl_name = key_split.pop(-1)
        namespace = ".".join([db_schema, *key_split])
        catalog.write_table_and_namespace(
            df=df,
            table_status=IcebergTableStatus.PROD,
            namespace=namespace,
            table_name=tbl_name,
        )

    datasets_context = get_projet_datasets_context(nom_projet=nom_projet)

    @task(map_index_template="{{ dataset_name }}")
    def print_dataset_context(
        dataset_context: DatasetContext,
        **context,
    ) -> None:
        context = get_current_context()
        context["dataset_name"] = dataset_context.dataset_name  # type: ignore
        print(f"Dataset: {dataset_context.dataset_name}")
        print(f"Source location: {dataset_context.src_location}")
        print(f"Temporary location: {dataset_context.tmp_location}")
        print(f"Destination location: {dataset_context.dest_location}")

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        datasets_context,
        [
            print_context(),
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
            config_projet_group(nom_projet=nom_projet),
            get_source_fichier_task(nom_projet=nom_projet),
            iceberg_task(),
            check_s3_hook(),
            check_trino_hook(),
            del_iceberg_staging_table(execution_options=execution_options),
            print_dataset_context.expand(dataset_context=datasets_context),
        ],
    )


dag_verification()
