from src.utils.tasks.etl import create_file_etl_task

from src.dags.sg.siep.mmsi.oad_referentiel import process

bien_typologie = create_file_etl_task(
    selecteur="ref_typologie",
    process_func=process.process_typologie_bien,
)
