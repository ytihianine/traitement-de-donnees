from pathlib import Path

from modules.domain.pipeline.model import ExecutionOptions

projet_dge_remuneration = "Cartographie rémunération"
config_projet_subpath = Path("dge/carto_rem")

execution_options = {
    "agent_info_carriere": ExecutionOptions(),
    "agent_remuneration": ExecutionOptions(),
}
