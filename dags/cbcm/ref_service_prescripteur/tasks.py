from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cbcm.ref_service_prescripteur import actions, config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


@task_group(group_id="grist_source")
def grist_source() -> None:
    ref_prog = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_prog"),),
            output_dataset=Dataset("ref_prog"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["prog"],
                custom_fn=process.process_ref_prog,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_prog"],
    )
    ref_bop = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_bop"),),
            output_dataset=Dataset("ref_bop"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["bop"],
                ref_columns=["prog"],
                custom_fn=process.process_ref_bop,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_bop"],
    )
    ref_uo = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_uo"),),
            output_dataset=Dataset("ref_uo"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["uo"],
                ref_columns=["prog", "bop"],
                custom_fn=process.process_ref_uo,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_uo"],
    )

    ref_cc = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_cc"),),
            output_dataset=Dataset("ref_cc"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["cc"],
                ref_columns=["prog", "bop", "uo"],
                custom_fn=process.process_ref_cc,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_cc"],
    )
    ref_sp_pilotage = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_sp_pilotage"),),
            output_dataset=Dataset("ref_sp_pilotage"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["service_prescripteur"],
                custom_fn=process.process_ref_sp_pilotage,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_sp_pilotage"],
    )
    ref_sp_choisi = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_sp_choisi"),),
            output_dataset=Dataset("ref_sp_choisi"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["service_prescripteur", "mail"],
                custom_fn=process.process_ref_sp_choisi,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_sp_choisi"],
    )
    ref_sdep = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_sdep"),),
            output_dataset=Dataset("ref_sdep"),
            operation=partial(
                generic_grist_processing,
                txt_columns=["service_depense"],
                custom_fn=process.process_ref_sdep,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["ref_sdep"],
    )
    sp = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("sp"),),
            output_dataset=Dataset("sp"),
            operation=partial(
                generic_grist_processing,
                cols_mapping={"centre_de_cout": "centre_cout"},
                txt_columns=[
                    "centre_financier",
                    "centre_cout",
                    "couple_cf_cc",
                    "observation",
                ],
                date_columns=["date_derniere_maj"],
                ref_columns=[
                    "service_prescripteur_pilotage_",
                    "service_depense",
                    "service_prescripteur_choisi_selon_cf_cc",
                    "designation_prog",
                    "designation_bop",
                    "designation_uo",
                    "designation_cc",
                ],
                custom_fn=process.process_sp,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["sp"],
    )
    # Services prescripteurs renseignés manuellement
    delai_global_paiement_sp_manuel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("delai_global_paiement_sp_manuel"),),
            output_dataset=Dataset("delai_global_paiement_sp_manuel"),
            operation=partial(
                generic_grist_processing,
                cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                ref_columns=["id_service_prescripteur"],
                custom_fn=process.process_delai_global_paiement_sp_manuel,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["delai_global_paiement_sp_manuel"],
    )
    demande_achat_sp_manuel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_achat_sp_manuel"),),
            output_dataset=Dataset("demande_achat_sp_manuel"),
            operation=partial(
                generic_grist_processing,
                cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                ref_columns=["id_service_prescripteur"],
                custom_fn=process.process_demande_achat_sp_manuel,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["demande_achat_sp_manuel"],
    )
    demande_paiement_sp_manuel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement_sp_manuel"),),
            output_dataset=Dataset("demande_paiement_sp_manuel"),
            operation=partial(
                generic_grist_processing,
                cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                ref_columns=["id_service_prescripteur"],
                custom_fn=process.process_demande_paiement_sp_manuel,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["demande_paiement_sp_manuel"],
    )
    engagement_juridique_sp_manuel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("engagement_juridique_sp_manuel"),),
            output_dataset=Dataset("engagement_juridique_sp_manuel"),
            operation=partial(
                generic_grist_processing,
                cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                ref_columns=["id_service_prescripteur"],
                custom_fn=process.process_engagement_juridique_sp_manuel,
            ),
            add_metadata=False,
        ),
        execution_options=config.execution_options["engagement_juridique_sp_manuel"],
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
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_all_cf_cc"),),
            output_dataset=Dataset("get_all_cf_cc"),
            operation=actions.get_all_cf_cc,
            add_metadata=False,
        ),
        execution_options=config.execution_options["get_all_cf_cc"],
    )
    get_demande_achat = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_demande_achat"),),
            output_dataset=Dataset("get_demande_achat"),
            operation=actions.get_demande_achat,
            add_metadata=False,
        ),
        execution_options=config.execution_options["get_demande_achat"],
    )
    get_demande_paiement_complet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_demande_paiement_complet"),),
            output_dataset=Dataset("get_demande_paiement_complet"),
            operation=actions.get_demande_paiement_complet,
            add_metadata=False,
        ),
        execution_options=config.execution_options["get_demande_paiement_complet"],
    )
    get_delai_global_paiement = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_delai_global_paiement"),),
            output_dataset=Dataset("get_delai_global_paiement"),
            operation=actions.get_delai_global_paiement,
            add_metadata=False,
        ),
        execution_options=config.execution_options["get_delai_global_paiement"],
    )
    get_engagement_juridique = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_engagement_juridique"),),
            output_dataset=Dataset("get_engagement_juridique"),
            operation=actions.get_engagement_juridique,
            add_metadata=False,
        ),
        execution_options=config.execution_options["get_engagement_juridique"],
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
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_all_cf_cc"), Dataset("sp")),
            output_dataset=Dataset("load_new_cf_cc"),
            operation=actions.load_new_cf_cc,
            use_input_results_as_operation_args=True,
            add_metadata=False,
        ),
        execution_options=config.execution_options["load_new_cf_cc"],
    )
    load_demande_achat = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_demande_achat"), Dataset("demande_achat_sp_manuel")),
            output_dataset=Dataset("load_demande_achat"),
            operation=actions.load_demande_achat,
            use_input_results_as_operation_args=True,
            add_metadata=False,
        ),
        execution_options=config.execution_options["load_demande_achat"],
    )
    load_demande_paiement_complet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_demande_paiement_complet"), Dataset("demande_paiement_sp_manuel")),
            output_dataset=Dataset("load_demande_paiement_complet"),
            operation=actions.load_demande_paiement_complet,
            use_input_results_as_operation_args=True,
            add_metadata=False,
        ),
        execution_options=config.execution_options["load_demande_paiement_complet"],
    )
    load_delai_global_paiement = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_delai_global_paiement"), Dataset("delai_global_paiement_sp_manuel")),
            output_dataset=Dataset("load_delai_global_paiement"),
            operation=actions.load_delai_global_paiement,
            use_input_results_as_operation_args=True,
            add_metadata=False,
        ),
        execution_options=config.execution_options["load_delai_global_paiement"],
    )
    load_engagement_juridique = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("get_engagement_juridique"), Dataset("engagement_juridique_sp_manuel")),
            output_dataset=Dataset("load_engagement_juridique"),
            operation=actions.load_engagement_juridique,
            add_metadata=False,
        ),
        execution_options=config.execution_options["load_engagement_juridique"],
    )

    chain(
        [
            load_new_cf_cc(),
            load_demande_achat(),
            load_demande_paiement_complet(),
            load_delai_global_paiement(),
            load_engagement_juridique(),
        ]
    )
