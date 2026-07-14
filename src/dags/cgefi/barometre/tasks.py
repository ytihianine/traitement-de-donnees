from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import TaskConfig
from src._types.readers import FileReaderStrategy
from src._types.tasks import ETLTask, SingleInputStep
from src._types.writers import FileWriterStrategy
from src.dags.cgefi.barometre import process

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


@task_group()
def source_files() -> None:
    cartographie = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    efc = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    recommandation = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    fiche_signaletique = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    rapport_annuel = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    organisme = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    organisme_hc = ETLTask(
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
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # ordre des tâches
    chain(
        [
            cartographie.create_task(),
            efc.create_task(),
            recommandation.create_task(),
            fiche_signaletique.create_task(),
            rapport_annuel.create_task(),
            organisme.create_task(),
            organisme_hc.create_task(),
        ]
    )
