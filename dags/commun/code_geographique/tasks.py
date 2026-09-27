from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.commun.code_geographique import actions, config
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


def _geo_pipeline(dataset_name: str, fn) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=fn,
    )


@task_group
def code_geographique() -> None:
    communes = create_task(
        pipeline=_geo_pipeline("communes", actions.communes),
        execution_options=config.execution_options["communes"],
    )
    departements = create_task(
        pipeline=_geo_pipeline("departements", actions.departements),
        execution_options=config.execution_options["departements"],
    )
    regions = create_task(
        pipeline=_geo_pipeline("regions", actions.regions),
        execution_options=config.execution_options["regions"],
    )
    chain(communes(), departements(), regions())


@task_group
def geojson() -> None:
    departements_geojson = create_task(
        pipeline=_geo_pipeline("departements_geojson", actions.departement_geojson),
        execution_options=config.execution_options["departements_geojson"],
    )
    regions_geojson = create_task(
        pipeline=_geo_pipeline("regions_geojson", actions.region_geojson),
        execution_options=config.execution_options["regions_geojson"],
    )
    chain([departements_geojson(), regions_geojson()])


@task_group
def code_iso() -> None:
    code_iso_departement = create_task(
        pipeline=_geo_pipeline("code_iso_departement", actions.code_iso_departement),
        execution_options=config.execution_options["code_iso_departement"],
    )
    code_iso_region = create_task(
        pipeline=_geo_pipeline("code_iso_region", actions.code_iso_region),
        execution_options=config.execution_options["code_iso_region"],
    )
    chain([code_iso_departement(), code_iso_region()])
