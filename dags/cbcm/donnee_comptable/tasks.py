from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cbcm.donnee_comptable import process
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, MultiInputStep, SingleInputStep
from modules.types.writers import FileWriterStrategy


@task_group(group_id="source_files")
def source_files() -> None:
    demande_achat = ETLTask(
        task_config=TaskConfig(task_id="demande_achat"),
        target="demande_achat",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_achat,
                input_key="demande_achat",
                output_key="demande_achat",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    engagement_juridique = ETLTask(
        task_config=TaskConfig(task_id="engagement_juridique"),
        target="engagement_juridique",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_engagement_juridique,
                input_key="engagement_juridique",
                output_key="engagement_juridique",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    demande_paiement = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement"),
        target="demande_paiement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_paiement,
                input_key="demande_paiement",
                output_key="demande_paiement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    demande_paiement_flux = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_flux"),
        target="demande_paiement_flux",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_paiement_flux,
                input_key="demande_paiement_flux",
                output_key="demande_paiement_flux",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    demande_paiement_sfp = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_sfp"),
        target="demande_paiement_sfp",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_paiement_sfp,
                input_key="demande_paiement_sfp",
                output_key="demande_paiement_sfp",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    demande_paiement_carte_achat = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_carte_achat"),
        target="demande_paiement_carte_achat",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_paiement_carte_achat,
                input_key="demande_paiement_carte_achat",
                output_key="demande_paiement_carte_achat",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    demande_paiement_journal_pieces = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_journal_pieces"),
        target="demande_paiement_journal_pieces",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_demande_paiement_journal_pieces,
                input_key="demande_paiement_journal_pieces",
                output_key="demande_paiement_journal_pieces",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    delai_global_paiement = ETLTask(
        task_config=TaskConfig(task_id="delai_global_paiement"),
        target="delai_global_paiement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_delai_global_paiement,
                input_key="delai_global_paiement",
                output_key="delai_global_paiement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    chain(
        [
            demande_achat.create_task(),
            engagement_juridique.create_task(),
            demande_paiement.create_task(),
            demande_paiement_flux.create_task(),
            demande_paiement_sfp.create_task(),
            demande_paiement_carte_achat.create_task(),
            demande_paiement_journal_pieces.create_task(),
            delai_global_paiement.create_task(),
        ]
    )


@task_group(group_id="dataset_additionnel")
def datasets_additionnels() -> None:
    demande_paiement_complet = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_complet"),
        target="demande_paiement_complet",
        reader=GristReaderStrategy(),
        steps=[
            MultiInputStep(
                fn=process.process_demande_paiement_complet,
                input_keys=[
                    "demande_paiement",
                    "demande_paiement_carte_achat",
                    "demande_paiement_flux",
                    "demande_paiement_journal_pieces",
                    "demande_paiement_sfp",
                ],
                output_key="demande_paiement_complet",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    chain(demande_paiement_complet.create_task())
