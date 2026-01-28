from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from utils.tasks.etl import (
    create_task,
    create_file_etl_task,
)

from dags.cbcm.donnee_comptable import process


@task_group(group_id="source_files")
def source_files() -> None:
    demande_achat = create_file_etl_task(
        selecteur="demande_achat",
        process_func=process.process_demande_achat,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )
    engagement_juridique = create_file_etl_task(
        selecteur="engagement_juridique",
        process_func=process.process_engagement_juridique,
        add_snapshot_id=False,
    )
    demande_paiement = create_file_etl_task(
        selecteur="demande_paiement",
        process_func=process.process_demande_paiement,
        add_snapshot_id=False,
    )
    demande_paiement_flux = create_file_etl_task(
        selecteur="demande_paiement_flux",
        process_func=process.process_demande_paiement_flux,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )
    demande_paiement_sfp = create_file_etl_task(
        selecteur="demande_paiement_sfp",
        process_func=process.process_demande_paiement_sfp,
        add_snapshot_id=False,
    )
    demande_paiement_carte_achat = create_file_etl_task(
        selecteur="demande_paiement_carte_achat",
        process_func=process.process_demande_paiement_carte_achat,
        add_snapshot_id=False,
    )
    demande_paiement_journal_pieces = create_file_etl_task(
        selecteur="demande_paiement_journal_pieces",
        process_func=process.process_demande_paiement_journal_pieces,
        add_snapshot_id=False,
    )
    delai_global_paiement = create_file_etl_task(
        selecteur="delai_global_paiement",
        process_func=process.process_delai_global_paiement,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )

    chain(
        [
            demande_achat(),
            engagement_juridique(),
            demande_paiement(),
            demande_paiement_flux(),
            demande_paiement_sfp(),
            demande_paiement_carte_achat(),
            demande_paiement_journal_pieces(),
            delai_global_paiement(),
        ]
    )


@task_group(group_id="dataset_additionnel")
def datasets_additionnels() -> None:
    demande_paiement_complet = create_task(
        task_config=TaskConfig(task_id="demande_paiement_complet"),
        output_selecteur="demande_paiement_complet",
        input_selecteurs=[
            "demande_paiement",
            "demande_paiement_carte_achat",
            "demande_paiement_flux",
            "demande_paiement_journal_pieces",
            "demande_paiement_sfp",
        ],
        steps=[ETLStep(fn=process.process_demande_paiement_complet, read_data=True)],
    )

    chain(demande_paiement_complet())
