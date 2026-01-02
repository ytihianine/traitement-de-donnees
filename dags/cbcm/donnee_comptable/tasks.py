from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS, ETLStep, TaskConfig
from utils.tasks.etl import (
    create_action_from_multi_input_files_etl_task,
    create_task,
    create_file_etl_task,
    create_multi_files_input_etl_task,
)

from dags.cbcm.donnee_comptable import process
from dags.cbcm.donnee_comptable import actions


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


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


add_new_sp = create_action_from_multi_input_files_etl_task(
    task_id="add_new_sp",
    input_selecteurs=[
        "demande_achat",
        "engagement_juridique",
        "demande_paiement_journal_pieces",
        "delai_global_paiement",
    ],
    action_func=actions.load_new_sp,
)

get_sp = create_task(
    task_config=TaskConfig(task_id="get_service_prescripteur"),
    output_selecteur="service_prescripteur",
    steps=[
        ETLStep(
            fn=actions.get_sp,
        )
    ],
    add_import_date=False,
    add_snapshot_id=False,
)


@task_group(group_id="ajout_sp")
def ajout_sp() -> None:
    demande_achat_sp = create_multi_files_input_etl_task(
        output_selecteur="demande_achat_sp",
        input_selecteurs=["demande_achat", "service_prescripteur"],
        process_func=process.add_service_prescripteurs,
    )
    engagement_juridique_sp = create_multi_files_input_etl_task(
        output_selecteur="engagement_juridique_sp",
        input_selecteurs=["engagement_juridique", "service_prescripteur"],
        process_func=process.add_service_prescripteurs,
    )
    demande_paiement_journal_pieces_sp = create_multi_files_input_etl_task(
        output_selecteur="demande_paiement_journal_pieces_sp",
        input_selecteurs=["demande_paiement_journal_pieces", "service_prescripteur"],
        process_func=process.add_service_prescripteurs,
    )
    delai_global_paiement_sp = create_multi_files_input_etl_task(
        output_selecteur="delai_global_paiement_sp",
        input_selecteurs=["delai_global_paiement", "service_prescripteur"],
        process_func=process.add_service_prescripteurs,
    )

    chain(
        [
            demande_achat_sp(),
            engagement_juridique_sp(),
            demande_paiement_journal_pieces_sp(),
            delai_global_paiement_sp(),
        ]
    )


@task_group(group_id="dataset_additionnel")
def datasets_additionnels() -> None:
    demande_paiement_complet_sp = create_multi_files_input_etl_task(
        output_selecteur="demande_paiement_complet_sp",
        input_selecteurs=[
            "demande_paiement",
            "demande_paiement_carte_achat",
            "demande_paiement_flux",
            "demande_paiement_journal_pieces",
            "demande_paiement_sfp",
        ],
        process_func=process.process_demande_paiement_complet_sp,
    )

    chain(demande_paiement_complet_sp())
