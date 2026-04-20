from _types.projet import SelecteurStorageOptions
from _enums.database import PartitionTimePeriod

selecteur_mapping = {
    "service": SelecteurStorageOptions(
        write_to_s3=True,
        write_to_s3_with_iceberg=True,
        write_to_db=True,
        tbl_order=0,
        is_partitioned=False,
    ),
    "direction": SelecteurStorageOptions(
        write_to_s3=True,
        write_to_s3_with_iceberg=True,
        write_to_db=False,
        tbl_order=0,
        is_partitioned=True,
        partition_period=PartitionTimePeriod.MONTH,
    ),
}
