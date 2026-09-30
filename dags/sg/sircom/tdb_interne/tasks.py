from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.sircom.tdb_interne import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
        ),
        add_metadata=False,
    )


@task_group(group_id="abonnes_visites")
def abonnes_visites() -> None:
    abonnes_reseaux_sociaux = create_task(
        pipeline=_grist_pipeline("abonnes_reseaux_sociaux", process.process_abonnes_reseaux_sociaux),
        execution_options=config.execution_options,
    )
    visites_portail = create_task(
        pipeline=_grist_pipeline("visites_portail", process.process_visites_portail),
        execution_options=config.execution_options,
    )
    visites_bercyinfo = create_task(
        pipeline=_grist_pipeline("visites_bercyinfo", process.process_visites_bercyinfo),
        execution_options=config.execution_options,
    )
    visites_alize = create_task(
        pipeline=_grist_pipeline("visites_alize", process.process_visites_alize),
        execution_options=config.execution_options,
    )
    visites_intranet_sg = create_task(
        pipeline=_grist_pipeline("visites_intranet_sg", process.process_visites_intranet_sg),
        execution_options=config.execution_options,
    )
    performances_lettres = create_task(
        pipeline=_grist_pipeline("performances_lettres", process.process_performances_lettres),
        execution_options=config.execution_options,
    )
    abonnes_aux_lettres = create_task(
        pipeline=_grist_pipeline("abonnes_aux_lettres", process.process_abonnes_aux_lettres),
        execution_options=config.execution_options,
    )
    ouverture_lettres_alize = create_task(
        pipeline=_grist_pipeline("ouverture_lettres_alize", process.process_ouverture_lettres_alize),
        execution_options=config.execution_options,
    )
    impressions_reseaux_sociaux = create_task(
        pipeline=_grist_pipeline("impressions_reseaux_sociaux", process.process_impressions_reseaux_sociaux),
        execution_options=config.execution_options,
    )
    impact_actions_com = create_task(
        pipeline=_grist_pipeline("impact_actions_com", process.process_impact_actions_com),
        execution_options=config.execution_options,
    )

    chain(
        [
            abonnes_reseaux_sociaux(),
            visites_portail(),
            visites_bercyinfo(),
            visites_alize(),
            visites_intranet_sg(),
            performances_lettres(),
            abonnes_aux_lettres(),
            ouverture_lettres_alize(),
            impressions_reseaux_sociaux(),
            impact_actions_com(),
        ]
    )


@task_group(group_id="budget")
def budget() -> None:
    synthese_depenses = create_task(
        pipeline=_grist_pipeline("synthese_depenses", process.process_synthese_depenses),
        execution_options=config.execution_options,
    )
    chain(synthese_depenses())


@task_group(group_id="enquetes")
def enquetes() -> None:
    engagement_agents_mef = create_task(
        pipeline=_grist_pipeline("engagement_agents_mef", process.process_engagement_agents_mef),
        execution_options=config.execution_options,
    )
    qualite_de_vie_au_travail = create_task(
        pipeline=_grist_pipeline("qualite_de_vie_au_travail", process.process_qualite_de_vie_au_travail),
        execution_options=config.execution_options,
    )
    collab_inter_structures = create_task(
        pipeline=_grist_pipeline("collab_inter_structures", process.process_collab_inter_structures),
        execution_options=config.execution_options,
    )
    observatoire_interne = create_task(
        pipeline=_grist_pipeline("observatoire_interne", process.process_observatoire_interne),
        execution_options=config.execution_options,
    )
    enquete_360 = create_task(
        pipeline=_grist_pipeline("enquete_360", process.process_enquete_360),
        execution_options=config.execution_options,
    )
    participation_observatoire_interne = create_task(
        pipeline=_grist_pipeline(
            "participation_observatoire_interne", process.process_participation_observatoire_interne
        ),
        execution_options=config.execution_options,
    )
    engagement_environnement = create_task(
        pipeline=_grist_pipeline("engagement_environnement", process.process_engagement_environnement),
        execution_options=config.execution_options,
    )

    chain(
        [
            engagement_agents_mef(),
            qualite_de_vie_au_travail(),
            collab_inter_structures(),
            observatoire_interne(),
            enquete_360(),
            participation_observatoire_interne(),
            engagement_environnement(),
        ]
    )


@task_group(group_id="metiers")
def metiers() -> None:
    indicateurs_metiers = create_task(
        pipeline=_grist_pipeline("indicateurs_metiers", process.process_indicateurs_metiers),
        execution_options=config.execution_options,
    )
    enquete_satisfaction = create_task(
        pipeline=_grist_pipeline("enquete_satisfaction", process.process_enquete_satisfaction),
        execution_options=config.execution_options,
    )
    etudes = create_task(
        pipeline=_grist_pipeline("etudes", process.process_etudes),
        execution_options=config.execution_options,
    )
    communique_presse = create_task(
        pipeline=_grist_pipeline("communique_presse", process.process_communique_presse),
        execution_options=config.execution_options,
    )
    creation_graphique = create_task(
        pipeline=_grist_pipeline("creation_graphique", process.process_creation_graphique),
        execution_options=config.execution_options,
    )
    notes_veilles = create_task(
        pipeline=_grist_pipeline("notes_veilles", process.process_notes_veilles),
        execution_options=config.execution_options,
    )
    recommandation_strat = create_task(
        pipeline=_grist_pipeline("recommandation_strat", process.process_recommandation_strat),
        execution_options=config.execution_options,
    )
    projets_graphiques = create_task(
        pipeline=_grist_pipeline("projets_graphiques", process.process_projets_graphiques),
        execution_options=config.execution_options,
    )

    chain(
        [
            indicateurs_metiers(),
            enquete_satisfaction(),
            etudes(),
            communique_presse(),
            creation_graphique(),
            notes_veilles(),
            recommandation_strat(),
            projets_graphiques(),
        ]
    )


@task_group(group_id="ressources_humaines")
def ressources_humaines() -> None:
    rh_formation = create_task(
        pipeline=_grist_pipeline("rh_formation", process.process_rh_formation),
        execution_options=config.execution_options,
    )
    rh_turnover = create_task(
        pipeline=_grist_pipeline("rh_turnover", process.process_rh_turnover),
        execution_options=config.execution_options,
    )
    rh_contractuel = create_task(
        pipeline=_grist_pipeline("rh_contractuel", process.process_rh_contractuel),
        execution_options=config.execution_options,
    )
    chain(
        [
            rh_formation(),
            rh_turnover(),
            rh_contractuel(),
        ]
    )
