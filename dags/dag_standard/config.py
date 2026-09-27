from modules.domain.pipeline.model import ExecutionOptions, PartitionTimePeriod

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
