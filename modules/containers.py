from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.database.repository.dataset_context import DbDatasetContextRepository
from modules.infra.database.repository.projet import DbProjetRepository
from modules.infra.file_system.dataset_location_factory import DatasetLocationFactory
from modules.infra.yaml.dataset_context import YamlDatasetContextRepository

# DEFAULT REPOSITORIES
DEFAULT_DAG_REPO = AirflowDagRepository()
DEFAULT_PROJET_REPO = DbProjetRepository()
DEFAULT_DATASET_CONTEXT_REPO = DbDatasetContextRepository()
YAML_DATASET_CONTEXT_REPO = YamlDatasetContextRepository()
DEFAULT_LOCATION_PROVIDER_FACTORY = DatasetLocationFactory()
