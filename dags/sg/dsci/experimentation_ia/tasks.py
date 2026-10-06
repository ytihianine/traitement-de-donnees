from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.experimentation_ia import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group()
def referentiels() -> None:
    # ==============================
    # referentiels commun à tous les questionnaires
    # ==============================
    ref_niveau_appropriation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_niveau_appropriation"),),
            output_dataset=Dataset(name="ref_niveau_appropriation"),
            operation=process.process_ref_niveau_appropriation,
        ),
        execution_options=config.execution_options,
    )
    ref_niveau_accord = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_niveau_accord"),),
            output_dataset=Dataset(name="ref_niveau_accord"),
            operation=process.process_ref_niveau_accord,
        ),
        execution_options=config.execution_options,
    )
    ref_formation_suivie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_formation_suivie"),),
            output_dataset=Dataset(name="ref_formation_suivie"),
            operation=process.process_ref_formation_suivie,
        ),
        execution_options=config.execution_options,
    )
    ref_participation_programme = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_participation_programme"),),
            output_dataset=Dataset(name="ref_participation_programme"),
            operation=process.process_ref_participation_programme,
        ),
        execution_options=config.execution_options,
    )

    # ==============================
    # referentiels du questionnaire 1
    # ==============================
    ref_direction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_direction"),),
            output_dataset=Dataset(name="ref_direction"),
            operation=process.process_ref_direction,
        ),
        execution_options=config.execution_options,
    )
    ref_domaine_professionnel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_domaine_professionnel"),),
            output_dataset=Dataset(name="ref_domaine_professionnel"),
            operation=process.process_ref_domaine_professionnel,
        ),
        execution_options=config.execution_options,
    )
    ref_cas_usage = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_cas_usage"),),
            output_dataset=Dataset(name="ref_cas_usage"),
            operation=process.process_ref_cas_usage,
        ),
        execution_options=config.execution_options,
    )

    # ==============================
    # referentiels du questionnaire 2 & 2 bis
    # ==============================
    ref_raison_perte_temps = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_raison_perte_temps"),),
            output_dataset=Dataset(name="ref_raison_perte_temps"),
            operation=process.process_ref_raison_perte_temps,
        ),
        execution_options=config.execution_options,
    )
    ref_impact_observation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_impact_observation"),),
            output_dataset=Dataset(name="ref_impact_observation"),
            operation=process.process_ref_impact_observation,
        ),
        execution_options=config.execution_options,
    )
    ref_impact_identifie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_impact_identifie"),),
            output_dataset=Dataset(name="ref_impact_identifie"),
            operation=process.process_ref_impact_identifie,
        ),
        execution_options=config.execution_options,
    )
    ref_taux_correction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_taux_correction"),),
            output_dataset=Dataset(name="ref_taux_correction"),
            operation=process.process_ref_taux_correction,
        ),
        execution_options=config.execution_options,
    )

    ref_type_erreur_ia = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_type_erreur_ia"),),
            output_dataset=Dataset(name="ref_type_erreur_ia"),
            operation=process.process_ref_type_erreur_ia,
        ),
        execution_options=config.execution_options,
    )
    ref_tache_realise = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_tache_realise"),),
            output_dataset=Dataset(name="ref_tache_realise"),
            operation=process.process_ref_tache_realise,
        ),
        execution_options=config.execution_options,
    )
    ref_facteur_progression = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_facteur_progression"),),
            output_dataset=Dataset(name="ref_facteur_progression"),
            operation=process.process_ref_facteur_progression,
        ),
        execution_options=config.execution_options,
    )
    ref_evolution_crainte = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_evolution_crainte"),),
            output_dataset=Dataset(name="ref_evolution_crainte"),
            operation=process.process_ref_evolution_crainte,
        ),
        execution_options=config.execution_options,
    )
    ref_frein_utilisation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_frein_utilisation"),),
            output_dataset=Dataset(name="ref_frein_utilisation"),
            operation=process.process_ref_frein_utilisation,
        ),
        execution_options=config.execution_options,
    )
    ref_raison_non_utilisation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_raison_non_utilisation"),),
            output_dataset=Dataset(name="ref_raison_non_utilisation"),
            operation=process.process_ref_raison_non_utilisation,
        ),
        execution_options=config.execution_options,
    )

    # ==============================
    # Référentiels du questionnaire 3
    # ==============================
    ref_raison_non_participation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_raison_non_participation"),),
            output_dataset=Dataset(name="ref_raison_non_participation"),
            operation=process.process_ref_raison_non_participation,
        ),
        execution_options=config.execution_options,
    )
    ref_levier_progression = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_levier_progression"),),
            output_dataset=Dataset(name="ref_levier_progression"),
            operation=process.process_ref_levier_progression,
        ),
        execution_options=config.execution_options,
    )
    ref_impact_tache_pro = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_impact_tache_pro"),),
            output_dataset=Dataset(name="ref_impact_tache_pro"),
            operation=process.process_ref_impact_tache_pro,
        ),
        execution_options=config.execution_options,
    )
    ref_impact_tache_rebarbative = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_impact_tache_rebarbative"),),
            output_dataset=Dataset(name="ref_impact_tache_rebarbative"),
            operation=process.process_ref_impact_tache_rebarbative,
        ),
        execution_options=config.execution_options,
    )
    ref_comparaison_autre_ia = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_comparaison_autre_ia"),),
            output_dataset=Dataset(name="ref_comparaison_autre_ia"),
            operation=process.process_ref_comparaison_autre_ia,
        ),
        execution_options=config.execution_options,
    )
    ref_autre_fonctionnalite = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_autre_fonctionnalite"),),
            output_dataset=Dataset(name="ref_autre_fonctionnalite"),
            operation=process.process_ref_autre_fonctionnalite,
        ),
        execution_options=config.execution_options,
    )
    ref_risque = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_risque"),),
            output_dataset=Dataset(name="ref_risque"),
            operation=process.process_ref_risque,
        ),
        execution_options=config.execution_options,
    )
    ref_besoin = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_besoin"),),
            output_dataset=Dataset(name="ref_besoin"),
            operation=process.process_ref_besoin,
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            ref_niveau_appropriation(),
            ref_niveau_accord(),
            ref_formation_suivie(),
            ref_participation_programme(),
            ref_direction(),
            ref_domaine_professionnel(),
            ref_cas_usage(),
            ref_raison_perte_temps(),
            ref_impact_observation(),
            ref_impact_identifie(),
            ref_taux_correction(),
            ref_type_erreur_ia(),
            ref_tache_realise(),
            ref_facteur_progression(),
            ref_evolution_crainte(),
            ref_frein_utilisation(),
            ref_raison_non_utilisation(),
            ref_raison_non_participation(),
            ref_levier_progression(),
            ref_impact_tache_pro(),
            ref_impact_tache_rebarbative(),
            ref_comparaison_autre_ia(),
            ref_autre_fonctionnalite(),
            ref_risque(),
            ref_besoin(),
        ]
    )


@task_group()
def experimentations() -> None:
    quota_par_entite = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="quota_par_entite"),),
            output_dataset=Dataset(name="quota_par_entite"),
            operation=process.process_quota_par_entite,
        ),
        execution_options=config.execution_options,
    )
    experimentateurs = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="experimentateurs"),),
            output_dataset=Dataset(name="experimentateurs"),
            operation=process.process_experimentateurs,
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([quota_par_entite(), experimentateurs()])


@task_group()
def suivi_questionnaire_1() -> None:
    q1 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q1"),), output_dataset=Dataset(name="q1"), operation=process.process_q1
        ),
        execution_options=config.execution_options,
    )
    q1_cas_usage = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q1_cas_usage"),),
            output_dataset=Dataset(name="q1_cas_usage"),
            operation=process.process_q1_cas_usage,
        ),
        execution_options=config.execution_options,
    )
    q1_besoins_accompagnement = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q1_besoins_accompagnement"),),
            output_dataset=Dataset(name="q1_besoins_accompagnement"),
            operation=process.process_q1_besoins_accompagnement,
        ),
        execution_options=config.execution_options,
    )

    # Ordre de tâches
    chain(
        [
            q1(),
            q1_cas_usage(),
            q1_besoins_accompagnement(),
        ]
    )


@task_group()
def suivi_questionnaire_2() -> None:
    q2 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2"),), output_dataset=Dataset(name="q2"), operation=process.process_q2
        ),
        execution_options=config.execution_options,
    )
    q2_typologie_interaction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_typologie_interaction"),),
            output_dataset=Dataset(name="q2_typologie_interaction"),
            operation=process.process_q2_typologie_interaction,
        ),
        execution_options=config.execution_options,
    )
    q2_formation_suivie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_formation_suivie"),),
            output_dataset=Dataset(name="q2_formation_suivie"),
            operation=process.process_q2_formation_suivie,
        ),
        execution_options=config.execution_options,
    )
    q2_participation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_participation"),),
            output_dataset=Dataset(name="q2_participation"),
            operation=process.process_q2_participation,
        ),
        execution_options=config.execution_options,
    )
    q2_freins = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_freins"),),
            output_dataset=Dataset(name="q2_freins"),
            operation=process.process_q2_freins,
        ),
        execution_options=config.execution_options,
    )
    q2_facteurs_progression = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_facteurs_progression"),),
            output_dataset=Dataset(name="q2_facteurs_progression"),
            operation=process.process_q2_facteurs_progression,
        ),
        execution_options=config.execution_options,
    )
    q2_taches = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_taches"),),
            output_dataset=Dataset(name="q2_taches"),
            operation=process.process_q2_taches,
        ),
        execution_options=config.execution_options,
    )
    q2_typologie_erreurs = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_typologie_erreurs"),),
            output_dataset=Dataset(name="q2_typologie_erreurs"),
            operation=process.process_q2_typologie_erreurs,
        ),
        execution_options=config.execution_options,
    )
    q2_impact_observe = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_impact_observe"),),
            output_dataset=Dataset(name="q2_impact_observe"),
            operation=process.process_q2_impact_observe,
        ),
        execution_options=config.execution_options,
    )
    q2_impact_identifie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2_impact_identifie"),),
            output_dataset=Dataset(name="q2_impact_identifie"),
            operation=process.process_q2_impact_identifie,
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            q2(),
            q2_typologie_interaction(),
            q2_formation_suivie(),
            q2_participation(),
            q2_freins(),
            q2_facteurs_progression(),
            q2_taches(),
            q2_typologie_erreurs(),
            q2_impact_observe(),
            q2_impact_identifie(),
        ]
    )


@task_group()
def suivi_questionnaire_2_bis() -> None:
    q2bis = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2bis"),),
            output_dataset=Dataset(name="q2bis"),
            operation=process.process_q2bis,
        ),
        execution_options=config.execution_options,
    )
    q2bis_raisons_non_utilisation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q2bis_raisons_non_utilisation"),),
            output_dataset=Dataset(name="q2bis_raisons_non_utilisation"),
            operation=process.process_q2bis_raisons_non_utilisation,
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain(
        [
            q2bis(),
            q2bis_raisons_non_utilisation(),
        ]
    )


@task_group()
def suivi_questionnaire_3() -> None:
    q3 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3"),), output_dataset=Dataset(name="q3"), operation=process.process_q3
        ),
        execution_options=config.execution_options,
    )
    q3_formation_suivie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_formation_suivie"),),
            output_dataset=Dataset(name="q3_formation_suivie"),
            operation=process.process_q3_formation_suivie,
        ),
        execution_options=config.execution_options,
    )
    q3_programme_rdv = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_programme_rdv"),),
            output_dataset=Dataset(name="q3_programme_rdv"),
            operation=process.process_q3_programme_rdv,
        ),
        execution_options=config.execution_options,
    )
    q3_leviers_progression = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_leviers_progression"),),
            output_dataset=Dataset(name="q3_leviers_progression"),
            operation=process.process_q3_leviers_progression,
        ),
        execution_options=config.execution_options,
    )

    q3_fonctionnalites = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_fonctionnalites"),),
            output_dataset=Dataset(name="q3_fonctionnalites"),
            operation=process.process_q3_fonctionnalites,
        ),
        execution_options=config.execution_options,
    )
    q3_risques_identifies = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_risques_identifies"),),
            output_dataset=Dataset(name="q3_risques_identifies"),
            operation=process.process_q3_risques_identifies,
        ),
        execution_options=config.execution_options,
    )
    q3_besoins_prioritaires = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_besoins_prioritaires"),),
            output_dataset=Dataset(name="q3_besoins_prioritaires"),
            operation=process.process_q3_besoins_prioritaires,
        ),
        execution_options=config.execution_options,
    )
    q3_besoins_moindres = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="q3_besoins_moindres"),),
            output_dataset=Dataset(name="q3_besoins_moindres"),
            operation=process.process_q3_besoins_moindres,
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            q3(),
            q3_formation_suivie(),
            q3_programme_rdv(),
            q3_leviers_progression(),
            q3_fonctionnalites(),
            q3_risques_identifies(),
            q3_besoins_prioritaires(),
            q3_besoins_moindres(),
        ]
    )


@task_group()
def tables_dimensions() -> None:
    dim_experimentateurs = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="experimentateurs"),
                Dataset(name="q1"),
                Dataset(name="q3"),
                Dataset(name="ref_direction"),
                Dataset(name="ref_domaine_professionnel"),
            ),
            output_dataset=Dataset(name="dim_experimentateurs"),
            operation=process.process_dim_experimentateurs,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_q2 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="experimentateurs"),
                Dataset(name="q1"),
                Dataset(name="q3"),
                Dataset(name="ref_direction"),
                Dataset(name="ref_domaine_professionnel"),
            ),
            output_dataset=Dataset(name="dim_q2"),
            operation=process.process_dim_q2,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_q3 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="experimentateurs"),
                Dataset(name="q1"),
                Dataset(name="q3"),
                Dataset(name="ref_direction"),
                Dataset(name="ref_domaine_professionnel"),
            ),
            output_dataset=Dataset(name="dim_q3"),
            operation=process.process_dim_q3,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            dim_experimentateurs(),
            dim_q2(),
            dim_q3(),
        ]
    )
