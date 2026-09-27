from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cgefi.barometre import process
from modules.infra.airflow.task import create_task
from modules.types.readers import FileReaderStrategy

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


@task_group()
def source_files() -> None:
    cartographie = create_task(
        task_config=TaskConfig(task_id="cartographie"),
        target="cartographie",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_cartographie,
                input_key="cartographie",
                output_key="cartographie",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    efc = create_task(
        task_config=TaskConfig(task_id="efc"),
        target="efc",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_efc,
                input_key="efc",
                output_key="efc",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    recommandation = create_task(
        task_config=TaskConfig(task_id="recommandation"),
        target="recommandation",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_recommandation,
                input_key="recommandation",
                output_key="recommandation",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    fiche_signaletique = create_task(
        task_config=TaskConfig(task_id="fiche_signaletique"),
        target="fiche_signaletique",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_fiche_signaletique,
                input_key="fiche_signaletique",
                output_key="fiche_signaletique",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    rapport_annuel = create_task(
        task_config=TaskConfig(task_id="rapport_annuel"),
        target="rapport_annuel",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_rapport_annuel,
                input_key="rapport_annuel",
                output_key="rapport_annuel",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    organisme = create_task(
        task_config=TaskConfig(task_id="organisme"),
        target="organisme",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_organisme,
                input_key="organisme",
                output_key="organisme",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
    )
    organisme_hc = create_task(
        task_config=TaskConfig(task_id="organisme_hors_corpus"),
        target="organisme_hors_corpus",
        reader=FileReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_organisme_hors_corpus,
                input_key="organisme_hors_corpus",
                output_key="organisme_hors_corpus",
            )
        ],
        writers=[FileDatasetWriter()],
        add_metadata=True,
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
