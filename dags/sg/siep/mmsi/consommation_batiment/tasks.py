from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from utils.tasks.file import create_parquet_converter_task
from utils.tasks.etl import create_file_etl_task, create_task

from dags.sg.siep.mmsi.consommation_batiment import process


conso_mens_parquet = create_parquet_converter_task(
    selecteur="conso_mens_source",
    task_params={"task_id": "convert_cons_mens_to_parquet"},
    process_func=process.process_source_conso_mens,
)


@task_group(group_id="source_files")
def source_files():
    informations_batiments = create_file_etl_task(
        selecteur="bien_info_complementaire",
        process_func=process.process_source_bien_info_comp,
        read_options={"sheet_name": 0},
    )
    conso_mensuelles = create_task(
        task_config=TaskConfig(task_id="conso_mens"),
        output_selecteur="conso_mens",
        input_selecteurs=["conso_mens_source"],
        steps=[ETLStep(fn=process.process_conso_mensuelles, read_data=True)],
    )
    chain([informations_batiments(), conso_mensuelles()])


@task_group(group_id="additionnal_files")
def additionnal_files():
    unpivot_conso_mens_corrigee = create_task(
        task_config=TaskConfig(task_id="conso_mens_corr_unpivot"),
        output_selecteur="conso_mens_corr_unpivot",
        input_selecteurs=["conso_mens"],
        steps=[ETLStep(fn=process.process_unpivot_conso_mens_corrigee, read_data=True)],
    )
    unpivot_conso_mens_brute = create_task(
        task_config=TaskConfig(task_id="conso_mens_brute_unpivot"),
        output_selecteur="conso_mens_brute_unpivot",
        input_selecteurs=["conso_mens"],
        steps=[ETLStep(fn=process.process_unpivot_conso_mens_brute, read_data=True)],
    )
    conso_annuelle = create_task(
        task_config=TaskConfig(task_id="conso_annuelle"),
        output_selecteur="conso_annuelle",
        input_selecteurs=["conso_mens"],
        steps=[ETLStep(fn=process.process_conso_annuelle, read_data=True)],
    )
    conso_annuelle_unpivot = create_task(
        task_config=TaskConfig(task_id="conso_annuelle_unpivot"),
        output_selecteur="conso_annuelle_unpivot",
        input_selecteurs=["conso_mens"],
        steps=[ETLStep(fn=process.process_conso_annuelle_unpivot, read_data=True)],
    )
    conso_annuelle_unpivot_comparaison = create_task(
        task_config=TaskConfig(task_id="conso_annuelle_unpivot_comparaison"),
        output_selecteur="conso_annuelle_unpivot_comparaison",
        input_selecteurs=["conso_annuelle_unpivot"],
        steps=[
            ETLStep(
                fn=process.process_conso_annuelle_unpivot_comparaison, read_data=True
            )
        ],
    )
    facture_annuelle_unpivot = create_task(
        task_config=TaskConfig(task_id="facture_annuelle_unpivot"),
        output_selecteur="facture_annuelle_unpivot",
        input_selecteurs=["conso_annuelle"],
        steps=[ETLStep(fn=process.process_facture_annuelle_unpivot, read_data=True)],
    )
    facture_annuelle_unpivot_comparaison = create_task(
        task_config=TaskConfig(task_id="facture_annuelle_unpivot_comparaison"),
        output_selecteur="facture_annuelle_unpivot_comparaison",
        input_selecteurs=["facture_annuelle_unpivot"],
        steps=[
            ETLStep(
                fn=process.process_facture_annuelle_unpivot_comparaison, read_data=True
            )
        ],
    )
    facture_annuelle_unpivot = create_task(
        task_config=TaskConfig(task_id="facture_annuelle_unpivot"),
        output_selecteur="facture_annuelle_unpivot",
        input_selecteurs=["conso_annuelle"],
        steps=[ETLStep(fn=process.process_facture_annuelle_unpivot, read_data=True)],
    )
    facture_annuelle_unpivot_comparaison = create_task(
        task_config=TaskConfig(task_id="facture_annuelle_unpivot_comparaison"),
        output_selecteur="facture_annuelle_unpivot_comparaison",
        input_selecteurs=["facture_annuelle_unpivot"],
        steps=[
            ETLStep(
                fn=process.process_facture_annuelle_unpivot_comparaison, read_data=True
            )
        ],
    )
    conso_statut_par_fluide = create_task(
        task_config=TaskConfig(task_id="conso_statut_par_fluide"),
        output_selecteur="conso_statut_par_fluide",
        input_selecteurs=["conso_annuelle"],
        steps=[ETLStep(fn=process.process_conso_statut_par_fluide, read_data=True)],
    )
    conso_avant_2019 = create_task(
        task_config=TaskConfig(task_id="conso_avant_2019"),
        output_selecteur="conso_avant_2019",
        input_selecteurs=["conso_annuelle"],
        steps=[ETLStep(fn=process.process_conso_avant_2019, read_data=True)],
    )
    conso_statut_fluide_global = create_task(
        task_config=TaskConfig(task_id="conso_statut_fluide_global"),
        output_selecteur="conso_statut_fluide_global",
        input_selecteurs=["conso_statut_par_fluide"],
        steps=[ETLStep(fn=process.process_conso_statut_fluide_global, read_data=True)],
    )
    conso_statut_batiment = create_task(
        task_config=TaskConfig(task_id="conso_statut_batiment"),
        output_selecteur="conso_statut_batiment",
        input_selecteurs=["conso_statut_fluide_global", "conso_avant_2019"],
        steps=[ETLStep(fn=process.process_conso_statut_batiment, read_data=True)],
    )

    chain(
        [
            unpivot_conso_mens_corrigee(),
            unpivot_conso_mens_brute(),
            conso_annuelle(),
            conso_annuelle_unpivot(),
        ],
        conso_annuelle_unpivot_comparaison(),
        facture_annuelle_unpivot(),
        facture_annuelle_unpivot_comparaison(),
        conso_statut_par_fluide(),
        conso_avant_2019(),
        conso_statut_fluide_global(),
        conso_statut_batiment(),
    )
