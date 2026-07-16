from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src._types.dags import TaskConfig
from src._types.readers import GristReaderStrategy
from src._types.tasks import ETLTask, SingleInputStep
from src._types.writers import FileWriterStrategy
from src.common_tasks.grist import generic_grist_processing
from src.dags.sg.dsci.carte_identite_mef import process


@task_group()
def effectif():
    teletravail = ETLTask(
        task_config=TaskConfig(task_id="teletravail"),
        target="teletravail",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_teletravail,
                ),
                input_key="teletravail",
                output_key="teletravail",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    teletravail_frequence = ETLTask(
        task_config=TaskConfig(task_id="teletravail_frequence"),
        target="teletravail_frequence",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_teletravail_frequence,
                ),
                input_key="teletravail_frequence",
                output_key="teletravail_frequence",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    teletravail_opinion = ETLTask(
        task_config=TaskConfig(task_id="teletravail_opinion"),
        target="teletravail_opinion",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_teletravail_opinion,
                ),
                input_key="teletravail_opinion",
                output_key="teletravail_opinion",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    effectif_direction = ETLTask(
        task_config=TaskConfig(task_id="effectif_direction"),
        target="effectif_direction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"nombre_d_agent": "nombre_agents"},
                    custom_fn=process.process_effectif_direction,
                ),
                input_key="effectif_direction",
                output_key="effectif_direction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    effectif_perimetre = ETLTask(
        task_config=TaskConfig(task_id="effectif_perimetre"),
        target="effectif_perimetre",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_effectif_perimetre,
                ),
                input_key="effectif_perimetre",
                output_key="effectif_perimetre",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    effectif_departements = ETLTask(
        task_config=TaskConfig(task_id="effectif_departements"),
        target="effectif_departements",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"nombre_d_agent": "nombre_agents"},
                    custom_fn=process.process_effectif_departements,
                ),
                input_key="effectif_departements",
                output_key="effectif_departements",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    masse_salariale = ETLTask(
        task_config=TaskConfig(task_id="masse_salariale"),
        target="masse_salariale",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"designation_du_ministere_ou_du_budget": "designation_ministere_ou_compte"},
                    custom_fn=process.process_masse_salariale,
                ),
                input_key="masse_salariale",
                output_key="masse_salariale",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    """ Task order """
    chain(
        [
            teletravail.create_task(),
            teletravail_frequence.create_task(),
            teletravail_opinion.create_task(),
            effectif_direction.create_task(),
            effectif_perimetre.create_task(),
            effectif_departements.create_task(),
            masse_salariale.create_task(),
        ]
    )


@task_group()
def budget():
    budget_total = ETLTask(
        task_config=TaskConfig(task_id="budget_total"),
        target="budget_total",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "etiquettes_de_lignes": "libelle",
                        "somme_de_cp_t2": "somme_cp_t2",
                        "somme_de_cp_hors_t2": "somme_cp_ht2",
                        "somme_de_cp_t2_ht2_bt": "somme_cp_t2_ht2_bt",
                        "part_du_total": "part_du_total",
                        "annee": "annee",
                        "type_budget": "type_budget",
                    },
                    custom_fn=process.process_budget_total,
                ),
                input_key="budget_total",
                output_key="budget_total",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    budget_pilotable = ETLTask(
        task_config=TaskConfig(task_id="budget_pilotable"),
        target="budget_pilotable",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_budget_pilotable,
                ),
                input_key="budget_pilotable",
                output_key="budget_pilotable",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    budget_general = ETLTask(
        task_config=TaskConfig(task_id="budget_general"),
        target="budget_general",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "etiquettes_de_lignes": "libelle",
                        "somme_de_cp_t2": "somme_cp_t2",
                        "somme_de_cp_hors_t2": "somme_cp_ht2",
                        "somme_de_cp_t2_ht2": "somme_cp_t2_ht2",
                        "part_du_total": "part_du_total",
                        "annee": "annee",
                        "type_budget": "type_budget",
                    },
                    custom_fn=process.process_budget_general,
                ),
                input_key="budget_general",
                output_key="budget_general",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    evolution_budget_mef = ETLTask(
        task_config=TaskConfig(task_id="evolution_budget_mef"),
        target="evolution_budget_mef",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_evolution_budget_mef,
                ),
                input_key="evolution_budget_mef",
                output_key="evolution_budget_mef",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    montant_intervention_invest = ETLTask(
        task_config=TaskConfig(task_id="montant_intervention_invest"),
        target="montant_intervention_invest",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "source": "source_montant",
                    },
                    custom_fn=process.process_montant_intervention_invest,
                ),
                input_key="montant_intervention_invest",
                output_key="montant_intervention_invest",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    budget_ministere = ETLTask(
        task_config=TaskConfig(task_id="budget_ministere"),
        target="budget_ministere",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "budgets_annexes": "budget_annexe",
                        "comptes_d_affectation_speciale": "compte_affection_speciale",
                        "comptes_de_concours_financiers": "compte_concours_financiers",
                        "total": "budget_total",
                    },
                    custom_fn=process.process_budget_ministere,
                ),
                input_key="budget_ministere",
                output_key="budget_ministere",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    """ Task order """
    chain(
        [
            budget_total.create_task(),
            budget_pilotable.create_task(),
            budget_general.create_task(),
            evolution_budget_mef.create_task(),
            montant_intervention_invest.create_task(),
            budget_ministere.create_task(),
        ]
    )


@task_group()
def taux_agent():
    engagement_agent = ETLTask(
        task_config=TaskConfig(task_id="engagement_agent"),
        target="engagement_agent",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_engagement_agent,
                ),
                input_key="engagement_agent",
                output_key="engagement_agent",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    election_resultat = ETLTask(
        task_config=TaskConfig(task_id="election_resultat"),
        target="election_resultat",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_election_resultat,
                ),
                input_key="election_resultat",
                output_key="election_resultat",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    taux_participation = ETLTask(
        task_config=TaskConfig(task_id="taux_participation"),
        target="taux_participation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_taux_participation,
                ),
                input_key="taux_participation",
                output_key="taux_participation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    """ Task order """
    chain(
        [
            engagement_agent.create_task(),
            election_resultat.create_task(),
            taux_participation.create_task(),
        ]
    )


@task_group()
def plafond():
    plafond_etpt = ETLTask(
        task_config=TaskConfig(task_id="plafond_etpt"),
        target="plafond_etpt",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "Designation_ministere_ou_budget_annexe": "designation_ministere_ou_budget_annexe",
                        "Plafond_en_ETPT": "plafond_en_etpt",
                        "Part_du_total": "part_du_total",
                    },
                    custom_fn=process.process_plafond_etpt,
                ),
                input_key="plafond_etpt",
                output_key="plafond_etpt",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    db_plafond_etpt = ETLTask(
        task_config=TaskConfig(task_id="db_plafond_etpt"),
        target="db_plafond_etpt",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
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
                    custom_fn=process.process_db_plafond_etpt,
                ),
                input_key="db_plafond_etpt",
                output_key="db_plafond_etpt",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    """ Task order """
    chain([plafond_etpt.create_task(), db_plafond_etpt.create_task()])
