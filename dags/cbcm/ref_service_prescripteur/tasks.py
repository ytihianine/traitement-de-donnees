from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.tasks.etl import create_grist_etl_task, create_task
from _types.dags import TaskConfig, ETLStep

from dags.cbcm.ref_service_prescripteur import process
from dags.cbcm.ref_service_prescripteur import actions
from utils.control.structures import normalize_grist_dataframe


@task_group(group_id="grist_source")
def grist_source() -> None:
    ref_prog = create_grist_etl_task(
        selecteur="ref_prog",
        process_func=process.process_ref_prog,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_bop = create_grist_etl_task(
        selecteur="ref_bop",
        process_func=process.process_ref_bop,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_uo = create_grist_etl_task(
        selecteur="ref_uo",
        process_func=process.process_ref_uo,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_cc = create_grist_etl_task(
        selecteur="ref_cc",
        process_func=process.process_ref_cc,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_sdep = create_grist_etl_task(
        selecteur="ref_sdep",
        process_func=process.process_ref_sdep,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_sp_choisi = create_grist_etl_task(
        selecteur="ref_sp_choisi",
        process_func=process.process_ref_sp_choisi,
        normalisation_process_func=normalize_grist_dataframe,
    )
    ref_sp_pilotage = create_grist_etl_task(
        selecteur="ref_sp_pilotage",
        process_func=process.process_ref_sp_pilotage,
        normalisation_process_func=normalize_grist_dataframe,
    )
    sp = create_grist_etl_task(
        selecteur="sp",
        process_func=process.process_service_prescripteur,
        normalisation_process_func=normalize_grist_dataframe,
    )
    # Services prescripteurs renseignés manuellement
    delai_global_paiement_sp_manuel = create_grist_etl_task(
        selecteur="delai_global_paiement_sp_manuel",
        process_func=process.process_delai_global_paiement_sp_manuel,
        normalisation_process_func=normalize_grist_dataframe,
    )
    demande_achat_sp_manuel = create_grist_etl_task(
        selecteur="demande_achat_sp_manuel",
        process_func=process.process_demande_achat_sp_manuel,
        normalisation_process_func=normalize_grist_dataframe,
    )
    demande_paiement_sp_manuel = create_grist_etl_task(
        selecteur="demande_paiement_sp_manuel",
        process_func=process.process_demande_paiement_sp_manuel,
        normalisation_process_func=normalize_grist_dataframe,
    )
    engagement_juridique_sp_manuel = create_grist_etl_task(
        selecteur="engagement_juridique_sp_manuel",
        process_func=process.process_engagement_juridique_sp_manuel,
        normalisation_process_func=normalize_grist_dataframe,
    )

    chain(
        [
            ref_prog(),
            ref_bop(),
            ref_uo(),
            ref_cc(),
            ref_sdep(),
            ref_sp_choisi(),
            ref_sp_pilotage(),
            sp(),
            delai_global_paiement_sp_manuel(),
            demande_achat_sp_manuel(),
            demande_paiement_sp_manuel(),
            engagement_juridique_sp_manuel(),
        ]
    )


@task_group(group_id="fetch_from_db")
def fetch_from_db() -> None:
    get_all_cf_cc = create_task(
        task_config=TaskConfig(task_id="get_all_cf_cc"),
        output_selecteur="get_all_cf_cc",
        steps=[
            ETLStep(
                fn=actions.get_all_cf_cc,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    get_demande_achat = create_task(
        task_config=TaskConfig(task_id="get_demande_achat"),
        output_selecteur="get_demande_achat",
        steps=[
            ETLStep(
                fn=actions.get_demande_achat,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    get_demande_paiement_complet = create_task(
        task_config=TaskConfig(task_id="get_demande_paiement_complet"),
        output_selecteur="get_demande_paiement_complet",
        steps=[
            ETLStep(
                fn=actions.get_demande_paiement_complet,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    get_delai_global_paiement = create_task(
        task_config=TaskConfig(task_id="get_delai_global_paiement"),
        output_selecteur="get_delai_global_paiement",
        steps=[
            ETLStep(
                fn=actions.get_delai_global_paiement,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )

    get_engagement_juridique = create_task(
        task_config=TaskConfig(task_id="get_engagement_juridique"),
        output_selecteur="get_engagement_juridique",
        steps=[
            ETLStep(
                fn=actions.get_engagement_juridique,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )

    chain(
        [
            get_all_cf_cc(),
            get_demande_achat(),
            get_demande_paiement_complet(),
            get_delai_global_paiement(),
            get_engagement_juridique(),
        ]
    )


@task_group(group_id="load_to_grist")
def load_to_grist() -> None:
    load_new_cf_cc = create_task(
        task_config=TaskConfig(task_id="load_new_cf_cc"),
        output_selecteur="load_new_cf_cc",
        input_selecteurs=["get_all_cf_cc", "sp"],
        steps=[ETLStep(fn=actions.load_new_cf_cc, read_data=True)],
        add_import_date=False,
        add_snapshot_id=False,
    )

    # load_demande_achat = create_task(
    #     task_config=TaskConfig(task_id="load_demande_achat"),
    #     output_selecteur="load_demande_achat",
    #     steps=[
    #         ETLStep(
    #             fn=actions.load_demande_achat,
    #         )
    #     ],
    #     add_import_date=False,
    #     add_snapshot_id=False,
    # )

    # load_demande_paiement_complet = create_task(
    #     task_config=TaskConfig(task_id="load_demande_paiement_complet"),
    #     output_selecteur="load_demande_paiement_complet",
    #     steps=[
    #         ETLStep(
    #             fn=actions.load_demande_paiement_complet,
    #         )
    #     ],
    #     add_import_date=False,
    #     add_snapshot_id=False,
    # )

    # load_delai_global_paiement = create_task(
    #     task_config=TaskConfig(task_id="load_delai_global_paiement"),
    #     output_selecteur="load_delai_global_paiement",
    #     steps=[
    #         ETLStep(
    #             fn=actions.load_delai_global_paiement,
    #         )
    #     ],
    #     add_import_date=False,
    #     add_snapshot_id=False,
    # )

    # load_engagement_juridique = create_task(
    #     task_config=TaskConfig(task_id="load_delai_global_paiement"),
    #     output_selecteur="load_delai_global_paiement",
    #     steps=[
    #         ETLStep(
    #             fn=actions.load_delai_global_paiement,
    #         )
    #     ],
    #     add_import_date=False,
    #     add_snapshot_id=False,
    # )

    chain(
        [
            load_new_cf_cc(),
            # load_demande_achat(),
            # load_demande_paiement_complet(),
            # load_delai_global_paiement(),
            # load_engagement_juridique(),
        ]
    )
