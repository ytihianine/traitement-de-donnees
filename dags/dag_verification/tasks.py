import logging
from pathlib import Path
from pprint import pprint

from airflow.sdk import Variable, chain, task, task_group
from dags.dag_verification.config import execution_options
from modules.constants import (
    AGENT,
    DEFAULT_GRIST_HOST,
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_HOST,
    DEFAULT_S3_CONN_ID,
    DEFAULT_TRINO_HOST,
    PROXY,
)
from modules.containers import (
    DEFAULT_DAG_REPO,
    DEFAULT_DATASET_CONTEXT_REPO,
    DEFAULT_PROJET_REPO,
)
from modules.domain.dag.model import DagConfig
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.projet import get_projet_datasets_context
from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.catalog.iceberg import (
    IcebergCatalog,
    generate_catalog_properties,
)
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler
from modules.infra.grist.client import GristClient
from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.factory import HttpHandlerType, create_http_client
from modules.infra.mails.default_smtp import MailMessage, MailStatus, _callback, send_mail


# =====================
# Domain verification
# =====================
@task_group
def check_dag(**context) -> None:
    @task
    def create_dag_config(**context) -> DagConfig:
        dag_repository = AirflowDagRepository()
        dag_config = DagConfig(
            nom_projet=dag_repository.get_project_name(context=context),
            dag_status=dag_repository.get_dag_status(context=context),
            db=dag_repository.get_db_info(context=context),
            enable=dag_repository.get_feature_flags(context=context),
        )
        logging.info(msg=f"DagConfig created: {dag_config}")
        return dag_config

    @task
    def retrieve_dag_info_from_context(**context) -> DagConfig:
        return DagConfig.from_dag_context(context_params=context["params"])

    @task
    def print_context(**context) -> None:
        pprint(object=context)
        pprint(object=context["dag"].__dict__)
        pprint(object=context["ti"].__dict__)

    chain(
        [
            create_dag_config(**context),
            retrieve_dag_info_from_context(**context),
            print_context(**context),
        ]
    )


@task_group
def check_projet(**context) -> None:
    @task
    def create_projet(nom_projet: str | None = None, **context) -> None:
        projet_repository = DEFAULT_PROJET_REPO
        if nom_projet is None:
            nom_projet = DEFAULT_DAG_REPO.get_project_name(context=context)

        projet = projet_repository.get(nom_projet=nom_projet)
        logging.info(msg=f"Projet retrieved: {projet}")

        metadata = projet_repository.get_projet_metadata(nom_projet=nom_projet)
        logging.info(msg=f"ProjetMetadata retrieved: {metadata}")

        contacts = projet_repository.get_list_contact(nom_projet=nom_projet)
        logging.info(msg=f"Contacts: {[c.contact_mail for c in contacts]}")

        documentation = projet_repository.get_list_documentation(nom_projet=nom_projet)
        logging.info(msg=f"Documentation: {[(d.type_documentation, d.lien) for d in documentation]}")

        s3_location = projet_repository.get_projet_location(nom_projet=nom_projet)
        logging.info(msg=f"S3 location: {s3_location}")

    @task
    def get_projet_metadata(nom_projet: str | None = None, **context) -> None:
        projet_repository = DEFAULT_PROJET_REPO
        if nom_projet is None:
            nom_projet = DEFAULT_DAG_REPO.get_project_name(context=context)

        metadata = projet_repository.get_projet_metadata(nom_projet=nom_projet)
        logging.info(msg=f"ProjetMetadata retrieved: {metadata}")

    chain(
        [
            create_projet(**context),
            get_projet_metadata(**context),
        ]
    )


@task_group
def check_dataset(nom_projet: str, **context) -> None:
    @task
    def get_dataset(nom_projet: str | None = None, **context) -> None:
        if nom_projet is None:
            nom_projet = DEFAULT_DAG_REPO.get_project_name(context=context)

        dataset_context_repository = DEFAULT_DATASET_CONTEXT_REPO
        datasets_context = dataset_context_repository.get_list(nom_projet=nom_projet)
        for dataset_context in datasets_context:
            logging.info(msg=(f"Dataset: {dataset_context}, "))

    datasets_context = get_projet_datasets_context(nom_projet=nom_projet)

    chain(
        [get_dataset(nom_projet=nom_projet, **context), datasets_context],
    )


def _noop_operation(df) -> None:
    return df


@task_group
def check_pipeline() -> None:
    @task
    def create_pipeline() -> None:
        pipeline_descriptor = PipelineDescriptor(
            input_datasets=(Dataset("ref_direction"),),
            output_dataset=Dataset("ref_direction"),
            operation=_noop_operation,
            add_metadata=False,
        )
        logging.info(msg=f"PipelineDescriptor created: {pipeline_descriptor}")
        logging.info(msg=f"Execution options: {execution_options}")

    chain(
        create_pipeline(),
    )


# =====================
# infra verification
# =====================
@task_group
def check_db_interface() -> None:
    @task
    def check_postgres() -> None:
        db_handler = create_db_handler(
            db_type=DatabaseType.POSTGRES,
            db_config=DbConfig(),
        )
        df = db_handler.fetch_df(query="SELECT 1 AS check;")
        logging.info(msg=f"Postgres check OK: {df.to_dict(orient='records')}")

    @task
    def check_sqlite() -> None:
        db_path = Path("/tmp") / "dag_verification_check.db"
        db_handler = create_db_handler(
            db_type=DatabaseType.SQLITE,
            db_config=DbConfig(db_path=str(db_path)),
        )
        db_handler.execute(query="CREATE TABLE IF NOT EXISTS verification (id INTEGER PRIMARY KEY, value TEXT);")
        db_handler.insert(table="verification", data={"id": 1, "value": "dag_verification"})
        rows = db_handler.fetch_all(query="SELECT * FROM verification;")
        logging.info(msg=f"SQLite check OK: {rows}")
        db_path.unlink(missing_ok=True)

    @task
    def check_trino() -> None:
        trino_user = Variable.get(key="TRINO_USER")
        db_handler = create_db_handler(
            db_type=DatabaseType.TRINO,
            db_config=DbConfig(
                host=DEFAULT_TRINO_HOST,
                user=trino_user,
                catalog=DEFAULT_POLARIS_CATALOG,
                port=443,
                http_scheme="https",
                verify=False,
            ),
        )
        logging.info(msg=f"Trino connection established to {DEFAULT_TRINO_HOST} as user {trino_user}")
        try:
            df = db_handler.fetch_df(query='SELECT * FROM "infrastructure.configuration.projet".direction')
            logging.info(msg=f"Trino check OK, {len(df)} rows")
            logging.info(msg=str(df.head()))
        except Exception as e:
            logging.error(msg=f"Trino check failed: {e}")

    chain(
        [
            check_postgres(),
            check_sqlite(),
            check_trino(),
        ]
    )


@task_group
def check_fs_interface() -> None:
    @task
    def check_s3() -> None:
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(connection_id=DEFAULT_S3_CONN_ID),
        )
        keys = s3_handler.list_files(directory="data_store/test_namespace/")
        logging.info(msg=f"S3 list_files: {len(keys)} files")
        keys_with_pattern = s3_handler.list_files(
            directory="data_store/test_namespace/",
            pattern="*_staging*",
        )
        logging.info(msg=f"S3 list_files with pattern: {len(keys_with_pattern)} files")
        s3_handler.delete_single(file_path="data_store/test_namespace/test_table_staging/")

    @task
    def check_local() -> None:
        base_path = Path("/tmp") / "dag_verification"
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(base_path=base_path),
        )
        test_file = "check_file.txt"
        local_handler.write(file_path=test_file, content="dag_verification")
        logging.info(msg=f"Local exists: {local_handler.exists(file_path=test_file)}")
        metadata = local_handler.get_metadata(file_path=test_file)
        logging.info(msg=f"Local metadata: name={metadata.name}, size={metadata.size}")
        listing = local_handler.list_files(directory=".")
        logging.info(msg=f"Local list_files: {listing}")
        local_handler.delete(file_path=test_file)
        logging.info(msg=f"Local exists after delete: {local_handler.exists(file_path=test_file)}")

    chain(
        [
            check_s3(),
            check_local(),
        ]
    )


@task_group
def check_iceberg_catalog() -> None:
    @task
    def check_catalog() -> None:
        properties = generate_catalog_properties(
            uri=DEFAULT_POLARIS_HOST,
        )
        try:
            catalog = IcebergCatalog(name=DEFAULT_POLARIS_CATALOG, properties=properties)
            logging.info(msg=f"Iceberg catalog connected: {catalog.name}")
        except Exception as e:
            logging.error(msg=f"Iceberg catalog connection failed: {e}")

    chain(
        check_catalog(),
    )


@task_group
def check_grist() -> None:
    @task
    def check_grist_client() -> None:
        http_config = ClientConfig()
        if PROXY:
            http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)

        grist_client = GristClient(
            http_client=create_http_client(
                client_type=HttpHandlerType.REQUEST,
                config=http_config,
            ),
            grist_host=DEFAULT_GRIST_HOST,
            api_token=Variable.get(key="grist_secret_key"),
        )
        response = grist_client.list_orgs()
        logging.info(msg=f"Grist list_orgs status: {response.status_code}")

    chain(
        check_grist_client(),
    )


@task_group
def check_http_interface() -> None:
    @task
    def check_requests_client() -> None:
        http_client = create_http_client(
            client_type=HttpHandlerType.REQUEST,
            config=ClientConfig(verify_ssl=False),
        )
        response = http_client.get(url="https://nubonyxia.incubateur.finances.rie.gouv.fr/")
        logging.info(msg=f"Requests client check OK: status={response.status_code}")
        http_client.close()

    @task
    def check_httpx_client() -> None:
        http_client = create_http_client(
            client_type=HttpHandlerType.HTTPX,
            config=ClientConfig(verify_ssl=False),
        )
        response = http_client.get(url="https://nubonyxia.incubateur.finances.rie.gouv.fr/")
        logging.info(msg=f"Httpx client check OK: status={response.status_code}")
        http_client.close()

    chain(
        [
            check_requests_client(),
            check_httpx_client(),
        ]
    )


@task_group
def check_smtp() -> None:
    @task
    def send_simple_mail() -> None:
        mail_message = MailMessage(
            to=["yanis.tihianine@finances.gouv.fr"],
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

    chain(
        [
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
        ]
    )
