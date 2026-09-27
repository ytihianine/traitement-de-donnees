from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.grist import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(generic_grist_processing, **grist_kwargs),
    )


@task_group
def referentiels() -> None:
    ref_base_remuneration = create_task(
        pipeline=_grist_pipeline(
            "ref_base_remuneration",
            txt_columns=["base_remuneration"],
        ),
        execution_options=config.execution_options["ref_base_remuneration"],
    )
    ref_base_revalorisation = create_task(
        pipeline=_grist_pipeline(
            "ref_base_revalorisation",
            txt_columns=["base_revalorisation"],
        ),
        execution_options=config.execution_options["ref_base_revalorisation"],
    )
    ref_niveau_diplome = create_task(
        pipeline=_grist_pipeline(
            "ref_niveau_diplome",
            txt_columns=["niveau_diplome"],
        ),
        execution_options=config.execution_options["ref_niveau_diplome"],
    )
    ref_valeur_point_indice = create_task(
        pipeline=_grist_pipeline(
            "ref_valeur_point_indice",
            date_columns=["date_d_application"],
        ),
        execution_options=config.execution_options["ref_valeur_point_indice"],
    )
    ref_categorie_ecole = create_task(
        pipeline=_grist_pipeline(
            "ref_categorie_ecole",
            txt_columns=["categorie_d_ecole"],
        ),
        execution_options=config.execution_options["ref_categorie_ecole"],
    )
    ref_libelle_diplome = create_task(
        pipeline=_grist_pipeline(
            "ref_libelle_diplome",
            cols_mapping={
                "categorie_ecole": "id_categorie_ecole",
                "niveau_diplome_associe": "id_niveau_diplome_associe",
            },
            txt_columns=["libelle_diplome"],
            ref_columns=["id_categorie_ecole", "id_niveau_diplome_associe"],
        ),
        execution_options=config.execution_options["ref_libelle_diplome"],
    )
    ref_position = create_task(
        pipeline=_grist_pipeline(
            "ref_position",
            cols_mapping={"niveau_diplome": "id_niveau_diplome"},
            ref_columns=["id_niveau_diplome"],
        ),
        execution_options=config.execution_options["ref_position"],
    )
    ref_fonction_dge = create_task(
        pipeline=_grist_pipeline(
            "ref_fonction_dge",
            txt_columns=["fonction_dge", "fonction_dge_libelle_long"],
        ),
        execution_options=config.execution_options["ref_fonction_dge"],
    )

    # ordre des tâches
    chain(
        [
            ref_base_remuneration(),
            ref_base_revalorisation(),
            ref_niveau_diplome(),
            ref_valeur_point_indice(),
            ref_categorie_ecole(),
            ref_libelle_diplome(),
            ref_position(),
            ref_fonction_dge(),
        ]
    )


@task_group
def source_grist() -> None:
    agent = create_task(
        pipeline=_grist_pipeline("agent"),
        execution_options=config.execution_options["agent"],
    )
    agent_diplome = create_task(
        pipeline=_grist_pipeline(
            "agent_diplome",
            cols_mapping={
                "niveau_diplome_associe": "id_niveau_diplome_associe",
                "categorie_d_ecole": "id_categorie_d_ecole",
                "libelle_diplome": "id_libelle_diplome",
            },
            ref_columns=[
                "id_libelle_diplome",
                "id_niveau_diplome_associe",
                "id_categorie_d_ecole",
            ],
        ),
        execution_options=config.execution_options["agent_diplome"],
    )
    agent_revalorisation = create_task(
        pipeline=_grist_pipeline(
            "agent_revalorisation",
            cols_mapping={"base_revalorisation": "id_base_revalorisation"},
            txt_columns=["historique"],
            date_columns=["date_dernier_renouvellement", "date_derniere_revalorisation"],
            ref_columns=["id_base_revalorisation"],
            custom_fn=process.process_agent_revalorisation,
        ),
        execution_options=config.execution_options["agent_revalorisation"],
    )
    agent_revalorisation_proposition = create_task(
        pipeline=_grist_pipeline(
            "agent_revalorisation_proposition",
            cols_mapping={"base_revalorisation": "id_base_revalorisation"},
            ref_columns=["id_base_revalorisation"],
        ),
        execution_options=config.execution_options["agent_revalorisation_proposition"],
    )
    agent_contrat_complement = create_task(
        pipeline=_grist_pipeline(
            "agent_contrat_complement",
            cols_mapping={
                "date_d_entree_a_la_dge": "date_entree_dge",
                "fonction_dge": "id_fonction_dge",
                "duree_contrat_en_cours": "duree_contrat_en_cours_dge",
                "duree_contrat_en_cours_auto": "duree_contrat_en_cours_auto_dge",
            },
            date_columns=[
                "date_premier_contrat_mef",
                "date_entree_dge",
                "date_de_cdisation",
            ],
            ref_columns=["id_fonction_dge"],
            custom_fn=process.process_agent_contrat_complement,
        ),
        execution_options=config.execution_options["agent_contrat_complement"],
    )
    agent_remuneration_complement = create_task(
        pipeline=_grist_pipeline(
            "agent_remuneration_complement",
            cols_mapping={
                "part_variable_collective": "plafond_part_variable_collective",
                "base_remuneration": "id_base_remuneration",
            },
            txt_columns=["observations"],
            ref_columns=["id_base_remuneration"],
            custom_fn=process.process_agent_remuneration_complement,
        ),
        execution_options=config.execution_options["agent_remuneration_complement"],
    )
    agent_experience_pro = create_task(
        pipeline=_grist_pipeline(
            "agent_experience_pro",
            cols_mapping={"position_grille": "id_position_grille"},
            ref_columns=["id_position_grille"],
            custom_fn=process.process_agent_experience_pro,
        ),
        execution_options=config.execution_options["agent_experience_pro"],
    )

    # ordre des tâches
    chain(
        [
            agent(),
            agent_diplome(),
            agent_revalorisation(),
            agent_revalorisation_proposition(),
            agent_contrat_complement(),
            agent_remuneration_complement(),
            agent_experience_pro(),
        ]
    )
