from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from types.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_task

from dags.commun.code_geographique import actions


@task_group
def code_geographique() -> None:
    communes = create_task(
        task_config=TaskConfig(task_id="communes"),
        output_selecteur="communes",
        steps=[
            ETLStep(
                fn=actions.communes,
            )
        ],
    )
    departements = create_task(
        task_config=TaskConfig(task_id="departements"),
        output_selecteur="departements",
        steps=[
            ETLStep(
                fn=actions.departements,
            )
        ],
    )
    regions = create_task(
        task_config=TaskConfig(task_id="regions"),
        output_selecteur="regions",
        steps=[
            ETLStep(
                fn=actions.regions,
            )
        ],
    )
    chain(
        communes(),
        departements(),
        regions(),
    )


@task_group
def geojson() -> None:
    departements_geojson = create_task(
        task_config=TaskConfig(task_id="departements_geojson"),
        output_selecteur="departements_geojson",
        steps=[
            ETLStep(
                fn=actions.departement_geojson,
            )
        ],
    )
    regions_geojson = create_task(
        task_config=TaskConfig(task_id="regions_geojson"),
        output_selecteur="regions_geojson",
        steps=[
            ETLStep(
                fn=actions.region_geojson,
            )
        ],
    )
    chain([departements_geojson(), regions_geojson()])


@task_group
def code_iso() -> None:
    departements_iso = create_task(
        task_config=TaskConfig(task_id="departements_iso"),
        output_selecteur="code_iso_departement",
        steps=[
            ETLStep(
                fn=actions.code_iso_departement,
            )
        ],
    )
    regions_iso = create_task(
        task_config=TaskConfig(task_id="regions_iso"),
        output_selecteur="code_iso_region",
        steps=[
            ETLStep(
                fn=actions.code_iso_region,
            )
        ],
    )
    chain([departements_iso(), regions_iso()])
