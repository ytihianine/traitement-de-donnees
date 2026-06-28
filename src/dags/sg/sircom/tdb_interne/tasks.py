from functools import partial
from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.common_tasks.grist import generic_grist_processing
from src._types.tasks import SingleInputStep, ETLTask
from src._types.readers import GristReaderStrategy
from src._types.writers import FileWriterStrategy
from src._types.dags import TaskConfig

from src.dags.sg.sircom.tdb_interne import process


@task_group(group_id="abonnes_visites")
def abonnes_visites() -> None:
    reseaux_sociaux = ETLTask(
        task_config=TaskConfig(task_id="reseaux_sociaux"),
        target="reseaux_sociaux",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_reseaux_sociaux,
                ),
                input_key="reseaux_sociaux",
                output_key="reseaux_sociaux",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    visites_portail = ETLTask(
        task_config=TaskConfig(task_id="visites_portail"),
        target="visites_portail",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_visites_portail,
                ),
                input_key="visites_portail",
                output_key="visites_portail",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    visites_bercyinfo = ETLTask(
        task_config=TaskConfig(task_id="visites_bercyinfo"),
        target="visites_bercyinfo",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_visites_bercyinfo,
                ),
                input_key="visites_bercyinfo",
                output_key="visites_bercyinfo",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    visites_alize = ETLTask(
        task_config=TaskConfig(task_id="visites_alize"),
        target="visites_alize",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_visites_alize,
                ),
                input_key="visites_alize",
                output_key="visites_alize",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    visites_intranet_sg = ETLTask(
        task_config=TaskConfig(task_id="visites_intranet_sg"),
        target="visites_intranet_sg",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_visites_intranet_sg,
                ),
                input_key="visites_intranet_sg",
                output_key="visites_intranet_sg",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    performances_lettres = ETLTask(
        task_config=TaskConfig(task_id="performances_lettres"),
        target="performances_lettres",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_performances_lettres,
                ),
                input_key="performances_lettres",
                output_key="performances_lettres",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    abonnes_lettres = ETLTask(
        task_config=TaskConfig(task_id="abonnes_lettres"),
        target="abonnes_lettres",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_abonnes_lettres,
                ),
                input_key="abonnes_lettres",
                output_key="abonnes_lettres",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ouverture_lettre_alize = ETLTask(
        task_config=TaskConfig(task_id="ouverture_lettre_alize"),
        target="ouverture_lettre_alize",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ouverture_lettre_alize,
                ),
                input_key="ouverture_lettre_alize",
                output_key="ouverture_lettre_alize",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    impressions_reseaux_sociaux = ETLTask(
        task_config=TaskConfig(task_id="impressions_reseaux_sociaux"),
        target="impressions_reseaux_sociaux",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_impressions_reseaux_sociaux,
                ),
                input_key="impressions_reseaux_sociaux",
                output_key="impressions_reseaux_sociaux",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    impact_actions_com = ETLTask(
        task_config=TaskConfig(task_id="impact_actions_com"),
        target="impact_actions_com",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_impact_actions_com,
                ),
                input_key="impact_actions_com",
                output_key="impact_actions_com",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )

    chain(
        [
            reseaux_sociaux.create_task(),
            visites_portail.create_task(),
            visites_bercyinfo.create_task(),
            visites_alize.create_task(),
            visites_intranet_sg.create_task(),
            performances_lettres.create_task(),
            abonnes_lettres.create_task(),
            ouverture_lettre_alize.create_task(),
            impressions_reseaux_sociaux.create_task(),
            impact_actions_com.create_task(),
        ]
    )


@task_group(group_id="budget")
def budget() -> None:
    synthese_depenses = ETLTask(
        task_config=TaskConfig(task_id="synthese_depenses"),
        target="synthese_depenses",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_synthese_depenses,
                ),
                input_key="synthese_depenses",
                output_key="synthese_depenses",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    chain(synthese_depenses.create_task())


@task_group(group_id="enquetes")
def enquetes() -> None:
    engagement_agents_mef = ETLTask(
        task_config=TaskConfig(task_id="engagement_agents_mef"),
        target="engagement_agents_mef",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_engagement_agents_mef,
                ),
                input_key="engagement_agents_mef",
                output_key="engagement_agents_mef",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    qualite_vie_travail = ETLTask(
        task_config=TaskConfig(task_id="qualite_vie_travail"),
        target="qualite_vie_travail",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_qualite_vie_travail,
                ),
                input_key="qualite_vie_travail",
                output_key="qualite_vie_travail",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    collab_inter_structure = ETLTask(
        task_config=TaskConfig(task_id="collab_inter_structure"),
        target="collab_inter_structure",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_collab_inter_structure,
                ),
                input_key="collab_inter_structure",
                output_key="collab_inter_structure",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    obs_interne = ETLTask(
        task_config=TaskConfig(task_id="obs_interne"),
        target="obs_interne",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_obs_interne,
                ),
                input_key="obs_interne",
                output_key="obs_interne",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    enquete_360 = ETLTask(
        task_config=TaskConfig(task_id="enquete_360"),
        target="enquete_360",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_enquete_360,
                ),
                input_key="enquete_360",
                output_key="enquete_360",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    obs_interne_participation = ETLTask(
        task_config=TaskConfig(task_id="obs_interne_participation"),
        target="obs_interne_participation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_obs_interne_participation,
                ),
                input_key="obs_interne_participation",
                output_key="obs_interne_participation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    engagement_environnement = ETLTask(
        task_config=TaskConfig(task_id="engagement_environnement"),
        target="engagement_environnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_engagement_environnement,
                ),
                input_key="engagement_environnement",
                output_key="engagement_environnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )

    chain(
        [
            engagement_agents_mef.create_task(),
            qualite_vie_travail.create_task(),
            collab_inter_structure.create_task(),
            obs_interne.create_task(),
            enquete_360.create_task(),
            obs_interne_participation.create_task(),
            engagement_environnement.create_task(),
        ]
    )


@task_group(group_id="metiers")
def metiers() -> None:
    indicateurs_metiers = ETLTask(
        task_config=TaskConfig(task_id="indicateurs_metiers"),
        target="indicateurs_metiers",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_indicateurs_metiers,
                ),
                input_key="indicateurs_metiers",
                output_key="indicateurs_metiers",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    enquete_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="enquete_satisfaction"),
        target="enquete_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_enquete_satisfaction,
                ),
                input_key="enquete_satisfaction",
                output_key="enquete_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    etudes = ETLTask(
        task_config=TaskConfig(task_id="etudes"),
        target="etudes",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_etudes,
                ),
                input_key="etudes",
                output_key="etudes",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    communique_presse = ETLTask(
        task_config=TaskConfig(task_id="communique_presse"),
        target="communique_presse",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_communique_presse,
                ),
                input_key="communique_presse",
                output_key="communique_presse",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    studio_graphique = ETLTask(
        task_config=TaskConfig(task_id="studio_graphique"),
        target="studio_graphique",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_studio_graphique,
                ),
                input_key="studio_graphique",
                output_key="studio_graphique",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    notes_veilles = ETLTask(
        task_config=TaskConfig(task_id="notes_veilles"),
        target="notes_veilles",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_notes_veilles,
                ),
                input_key="notes_veilles",
                output_key="notes_veilles",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    recommandation_strat = ETLTask(
        task_config=TaskConfig(task_id="recommandation_strat"),
        target="recommandation_strat",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_recommandation_strat,
                ),
                input_key="recommandation_strat",
                output_key="recommandation_strat",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    projets_graphiques = ETLTask(
        task_config=TaskConfig(task_id="projets_graphiques"),
        target="projets_graphiques",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_projets_graphiques,
                ),
                input_key="projets_graphiques",
                output_key="projets_graphiques",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )

    chain(
        [
            indicateurs_metiers.create_task(),
            enquete_satisfaction.create_task(),
            etudes.create_task(),
            communique_presse.create_task(),
            studio_graphique.create_task(),
            notes_veilles.create_task(),
            recommandation_strat.create_task(),
            projets_graphiques.create_task(),
        ]
    )


@task_group(group_id="ressources_humaines")
def ressources_humaines() -> None:
    rh_formation = ETLTask(
        task_config=TaskConfig(task_id="rh_formation"),
        target="rh_formation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_rh_formation,
                ),
                input_key="rh_formation",
                output_key="rh_formation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    rh_turnover = ETLTask(
        task_config=TaskConfig(task_id="rh_turnover"),
        target="rh_turnover",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_rh_turnover,
                ),
                input_key="rh_turnover",
                output_key="rh_turnover",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    rh_contractuel = ETLTask(
        task_config=TaskConfig(task_id="rh_contractuel"),
        target="rh_contractuel",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_rh_contractuel,
                ),
                input_key="rh_contractuel",
                output_key="rh_contractuel",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    chain(
        [
            rh_formation.create_task(),
            rh_turnover.create_task(),
            rh_contractuel.create_task(),
        ]
    )
