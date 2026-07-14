from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from src.common_tasks.etl import create_task
from src.dags.cgefi.barometre import process

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


@task_group()
def source_files() -> None:
    cartographie = create_task(
        task_config=TaskConfig(task_id="cartographie"),
        output_selecteur="cartographie",
        input_selecteurs=["cartographie"],
        steps=[ETLStep(fn=process.process_cartographie, read_data=True)],
    )
    efc = create_task(
        task_config=TaskConfig(task_id="efc"),
        output_selecteur="efc",
        input_selecteurs=["efc"],
        steps=[ETLStep(fn=process.process_efc, read_data=True)],
    )
    recommandation = create_task(
        task_config=TaskConfig(task_id="recommandation"),
        output_selecteur="recommandation",
        input_selecteurs=["recommandation"],
        steps=[ETLStep(fn=process.process_recommandation, read_data=True)],
    )
    fiche_signaletique = create_task(
        task_config=TaskConfig(task_id="fiche_signaletique"),
        output_selecteur="fiche_signaletique",
        input_selecteurs=["fiche_signaletique"],
        steps=[ETLStep(fn=process.process_fiches_signaletiques, read_data=True)],
    )
    rapport_annuel = create_task(
        task_config=TaskConfig(task_id="rapport_annuel"),
        output_selecteur="rapport_annuel",
        input_selecteurs=["rapport_annuel"],
        steps=[ETLStep(fn=process.process_rapport_annuel, read_data=True)],
    )
    organisme = create_task(
        task_config=TaskConfig(task_id="organisme"),
        output_selecteur="organisme",
        input_selecteurs=["organisme"],
        steps=[ETLStep(fn=process.process_organisme, read_data=True)],
    )
    organisme_hc = create_task(
        task_config=TaskConfig(task_id="organisme_hors_corpus"),
        output_selecteur="organisme_hors_corpus",
        input_selecteurs=["organisme_hors_corpus"],
        steps=[ETLStep(fn=process.process_organisme_hors_corpus, read_data=True)],
    )

    # ordre des tâches
    chain(
        [
            cartographie(),
            efc(),
            recommandation(),
            fiche_signaletique(),
            rapport_annuel(),
            organisme(),
            organisme_hc(),
        ]
    )
