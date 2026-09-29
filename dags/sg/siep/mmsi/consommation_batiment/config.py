from modules.domain.pipeline.model import ExecutionOptions

dag_id_osfi = "consommation_des_batiments"
nom_projet_osfi = "Consommation des bâtiments"

execution_options = {
    "conso_mens_source": ExecutionOptions(),
    "conso_avant_2019": ExecutionOptions(),
    "conso_statut_fluide_global": ExecutionOptions(),
    "bien_info_complementaire": ExecutionOptions(read_options={"sheet_name": 0}),
}
