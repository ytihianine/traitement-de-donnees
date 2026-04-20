from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.common_tasks.etl import create_grist_etl_task
from src.utils.process.structures import normalize_grist_dataframe


from src.dags.sg.sircom.tdb_interne import process

version = "v1"


@task_group(group_id="abonnes_visites")
def abonnes_visites() -> None:
    reseaux_sociaux = create_grist_etl_task(
        selecteur="reseaux_sociaux",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_reseaux_sociaux,
        version=version,
    )
    visites_portail = create_grist_etl_task(
        selecteur="visites_portail",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_visites_portail,
        version=version,
    )
    visites_bercyinfo = create_grist_etl_task(
        selecteur="visites_bercyinfo",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_visites_bercyinfo,
        version=version,
    )
    visites_alize = create_grist_etl_task(
        selecteur="visites_alize",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_visites_alize,
        version=version,
    )
    visites_intranet_sg = create_grist_etl_task(
        selecteur="visites_intranet_sg",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_visites_intranet_sg,
        version=version,
    )
    performances_lettres = create_grist_etl_task(
        selecteur="performances_lettres",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_performances_lettres,
        version=version,
    )
    abonnes_aux_lettres = create_grist_etl_task(
        selecteur="abonnes_lettres",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_abonnes_aux_lettres,
        version=version,
    )
    ouverture_lettre_alize = create_grist_etl_task(
        selecteur="ouverture_lettre_alize",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ouverture_lettre_alize,
        version=version,
    )
    impressions_reseaux_sociaux = create_grist_etl_task(
        selecteur="impressions_reseaux_sociaux",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_impressions_reseaux_sociaux,
        version=version,
    )
    impact_actions_com = create_grist_etl_task(
        selecteur="impact_actions_com",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_impact_actions_com,
        version=version,
    )

    chain(
        [
            reseaux_sociaux(),
            visites_portail(),
            visites_bercyinfo(),
            visites_alize(),
            visites_intranet_sg(),
            performances_lettres(),
            abonnes_aux_lettres(),
            ouverture_lettre_alize(),
            impressions_reseaux_sociaux(),
            impact_actions_com(),
        ]
    )


@task_group(group_id="budget")
def budget() -> None:
    budget_depense = create_grist_etl_task(
        selecteur="synthese_depenses",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_budget_depense,
        version=version,
    )
    chain(budget_depense())


@task_group(group_id="enquetes")
def enquetes() -> None:
    engagement_agents_mef = create_grist_etl_task(
        selecteur="engagement_agents_mef",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_engagement_agents_mef,
        version=version,
    )
    qualite_vie_travail = create_grist_etl_task(
        selecteur="qualite_vie_travail",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_qualite_vie_travail,
        version=version,
    )
    collab_inter_structure = create_grist_etl_task(
        selecteur="collab_inter_structure",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_collab_inter_structure,
        version=version,
    )
    obs_interne = create_grist_etl_task(
        selecteur="obs_interne",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_obs_interne,
        version=version,
    )
    enquete_360 = create_grist_etl_task(
        selecteur="enquete_360",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_enquete_360,
        version=version,
    )
    obs_interne_participation = create_grist_etl_task(
        selecteur="obs_interne_participation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_obs_interne_participation,
        version=version,
    )
    engagement_environnement = create_grist_etl_task(
        selecteur="engagement_environnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_engagement_environnement,
        version=version,
    )
    chain(
        [
            engagement_agents_mef(),
            qualite_vie_travail(),
            collab_inter_structure(),
            obs_interne(),
            enquete_360(),
            obs_interne_participation(),
            engagement_environnement(),
        ]
    )


@task_group(group_id="metiers")
def metiers() -> None:
    indicateurs_metiers = create_grist_etl_task(
        selecteur="indicateurs_metiers",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_indicateurs_metiers,
        version=version,
    )
    enquete_satisfaction = create_grist_etl_task(
        selecteur="enquete_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_enquete_satisfaction,
        version=version,
    )
    etudes = create_grist_etl_task(
        selecteur="etudes",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_etudes,
        version=version,
    )
    communique_presse = create_grist_etl_task(
        selecteur="communique_presse",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_communique_presse,
        version=version,
    )
    studio_graphique = create_grist_etl_task(
        selecteur="studio_graphique",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_studio_graphique,
        version=version,
    )
    notes_veilles = create_grist_etl_task(
        selecteur="notes_veilles",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_notes_veilles,
        version=version,
    )
    recommandation_strat = create_grist_etl_task(
        selecteur="recommandation_strat",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_recommandation_strat,
        version=version,
    )
    projets_graphiques = create_grist_etl_task(
        selecteur="projets_graphiques",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projets_graphiques,
        version=version,
    )
    chain(
        [
            indicateurs_metiers(),
            enquete_satisfaction(),
            etudes(),
            communique_presse(),
            studio_graphique(),  # [OLD] ref projets_graphique
            notes_veilles(),
            recommandation_strat(),
            projets_graphiques(),
        ]
    )


@task_group(group_id="ressources_humaines")
def ressources_humaines() -> None:
    rh_formation = create_grist_etl_task(
        selecteur="rh_formation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_rh_formation,
        version=version,
    )
    rh_turnover = create_grist_etl_task(
        selecteur="rh_turnover",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_rh_turnover,
        version=version,
    )
    rh_contractuel = create_grist_etl_task(
        selecteur="rh_contractuel",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_rh_contractuel,
        version=version,
    )
    chain([rh_formation(), rh_turnover(), rh_contractuel()])
