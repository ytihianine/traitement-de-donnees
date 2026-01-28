from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.tasks.etl import create_grist_etl_task

from dags.cbcm.ref_service_prescripteur import process
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
