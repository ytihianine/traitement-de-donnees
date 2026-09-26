from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.database.repository.dataset_context import DbDatasetContextRepository
from modules.infra.database.repository.projet import DbProjetRepository

# DEFAULT REPOSITORIES
DEFAULT_DAG_REPO = AirflowDagRepository()
DEFAULT_PROJET_REPO = DbProjetRepository()
DEFAULT_DATASET_CONTEXT_REPO = DbDatasetContextRepository()
