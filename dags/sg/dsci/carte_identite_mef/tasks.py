from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.carte_identite_mef import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
            **grist_kwargs,
        ),
    )


@task_group()
def effectif():
    teletravail = create_task(
        pipeline=_grist_pipeline("teletravail", process.process_teletravail),
        execution_options=config.execution_options["teletravail"],
    )
    teletravail_frequence = create_task(
        pipeline=_grist_pipeline("teletravail_frequence", process.process_teletravail_frequence),
        execution_options=config.execution_options["teletravail_frequence"],
    )
    teletravail_opinion = create_task(
        pipeline=_grist_pipeline("teletravail_opinion", process.process_teletravail_opinion),
        execution_options=config.execution_options["teletravail_opinion"],
    )
    effectif_direction = create_task(
        pipeline=_grist_pipeline(
            "effectif_direction",
            process.process_effectif_direction,
            cols_mapping={"nombre_d_agent": "nombre_agents"},
        ),
        execution_options=config.execution_options["effectif_direction"],
    )
    effectif_perimetre = create_task(
        pipeline=_grist_pipeline("effectif_perimetre", process.process_effectif_perimetre),
        execution_options=config.execution_options["effectif_perimetre"],
    )
    effectif_departements = create_task(
        pipeline=_grist_pipeline(
            "effectif_departements",
            process.process_effectif_departements,
            cols_mapping={"nombre_d_agent": "nombre_agents"},
        ),
        execution_options=config.execution_options["effectif_departements"],
    )
    masse_salariale = create_task(
        pipeline=_grist_pipeline(
            "masse_salariale",
            process.process_masse_salariale,
            cols_mapping={"designation_du_ministere_ou_du_budget": "designation_ministere_ou_compte"},
        ),
        execution_options=config.execution_options["masse_salariale"],
    )

    """ Task order """
    chain(
        [
            teletravail(),
            teletravail_frequence(),
            teletravail_opinion(),
            effectif_direction(),
            effectif_perimetre(),
            effectif_departements(),
            masse_salariale(),
        ]
    )


@task_group()
def budget():
    budget_total = create_task(
        pipeline=_grist_pipeline(
            "budget_total",
            process.process_budget_total,
            cols_mapping={
                "etiquettes_de_lignes": "libelle",
                "somme_de_cp_t2": "somme_cp_t2",
                "somme_de_cp_hors_t2": "somme_cp_ht2",
                "somme_de_cp_t2_ht2_bt": "somme_cp_t2_ht2_bt",
                "part_du_total": "part_du_total",
                "annee": "annee",
                "type_budget": "type_budget",
            },
        ),
        execution_options=config.execution_options["budget_total"],
    )
    budget_pilotable = create_task(
        pipeline=_grist_pipeline("budget_pilotable", process.process_budget_pilotable),
        execution_options=config.execution_options["budget_pilotable"],
    )
    budget_general = create_task(
        pipeline=_grist_pipeline(
            "budget_general",
            process.process_budget_general,
            cols_mapping={
                "etiquettes_de_lignes": "libelle",
                "somme_de_cp_t2": "somme_cp_t2",
                "somme_de_cp_hors_t2": "somme_cp_ht2",
                "somme_de_cp_t2_ht2": "somme_cp_t2_ht2",
                "part_du_total": "part_du_total",
                "annee": "annee",
                "type_budget": "type_budget",
            },
        ),
        execution_options=config.execution_options["budget_general"],
    )
    evolution_budget_mef = create_task(
        pipeline=_grist_pipeline("evolution_budget_mef", process.process_evolution_budget_mef),
        execution_options=config.execution_options["evolution_budget_mef"],
    )
    montant_intervention_invest = create_task(
        pipeline=_grist_pipeline(
            "montant_intervention_invest",
            process.process_montant_intervention_invest,
            cols_mapping={
                "source": "source_montant",
            },
        ),
        execution_options=config.execution_options["montant_intervention_invest"],
    )
    budget_ministere = create_task(
        pipeline=_grist_pipeline(
            "budget_ministere",
            process.process_budget_ministere,
            cols_mapping={
                "budgets_annexes": "budget_annexe",
                "comptes_d_affectation_speciale": "compte_affection_speciale",
                "comptes_de_concours_financiers": "compte_concours_financiers",
                "total": "budget_total",
            },
        ),
        execution_options=config.execution_options["budget_ministere"],
    )

    """ Task order """
    chain(
        [
            budget_total(),
            budget_pilotable(),
            budget_general(),
            evolution_budget_mef(),
            montant_intervention_invest(),
            budget_ministere(),
        ]
    )


@task_group()
def taux_agent():
    engagement_agent = create_task(
        pipeline=_grist_pipeline("engagement_agent", process.process_engagement_agent),
        execution_options=config.execution_options["engagement_agent"],
    )
    election_resultat = create_task(
        pipeline=_grist_pipeline("election_resultat", process.process_election_resultat),
        execution_options=config.execution_options["election_resultat"],
    )
    taux_participation = create_task(
        pipeline=_grist_pipeline("taux_participation", process.process_taux_participation),
        execution_options=config.execution_options["taux_participation"],
    )

    """ Task order """
    chain(
        [
            engagement_agent(),
            election_resultat(),
            taux_participation(),
        ]
    )


@task_group()
def plafond():
    plafond_etpt = create_task(
        pipeline=_grist_pipeline(
            "plafond_etpt",
            process.process_plafond_etpt,
            cols_mapping={
                "Designation_ministere_ou_budget_annexe": "designation_ministere_ou_budget_annexe",
                "Plafond_en_ETPT": "plafond_en_etpt",
                "Part_du_total": "part_du_total",
            },
        ),
        execution_options=config.execution_options["plafond_etpt"],
    )
    db_plafond_etpt = create_task(
        pipeline=_grist_pipeline(
            "db_plafond_etpt",
            process.process_db_plafond_etpt,
            cols_mapping={
                "Source": "source",
                "Annee": "annee",
                "Type_budgetaire": "type_budgetaire",
                "Type_de_valeur": "type_de_valeur",
                "Type_de_budget": "type_de_budget",
                "Designation_ministere_ou_budget_annexe": "designation_ministere_ou_budget_annexe",
                "Valeur": "valeur",
                "Part_du_total": "part_du_total",
                "Unite": "unite",
            },
        ),
        execution_options=config.execution_options["db_plafond_etpt"],
    )
    """ Task order """
    chain([plafond_etpt(), db_plafond_etpt()])
