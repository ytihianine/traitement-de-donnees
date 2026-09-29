from modules.domain.pipeline.model import ExecutionOptions, PartitionTimePeriod

nom_projet = "Configuration des projets"
nom_projet_test = "Configuration des projets"

execution_options = {
    "service": ExecutionOptions(
        tbl_order=0,
        is_partitioned=False,
    ),
    "direction": ExecutionOptions(
        tbl_order=0,
        is_partitioned=True,
        partition_period=PartitionTimePeriod.MONTH,
    ),
}
