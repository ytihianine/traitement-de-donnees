from datetime import datetime, timedelta

from scripts.handle_partitions.commun import Actions

# Database
schema = "siep"
tbl_to_keep = ("conso", "bien_information", "facture")
action = Actions.DROP
dry_run = True

# Si Actions.DROP
drop_from_date = (
    None  # else datetime.strptime("31/07/2025 00:00:00", "%d/%m/%Y %H:%M:%S")
)


# Si Actions.CREATE
str_date = "31/07/2025 00:00:00"
create_from_date = datetime.strptime(str_date, "%d/%m/%Y %H:%M:%S")
to_date = create_from_date + timedelta(days=1)
print(create_from_date, to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2025-12-20 22:55:24", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2025-07-31 12:00:00", "%Y-%m-%d %H:%M:%S")
print(curr_import_timestamp, new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20251220_22:55:24"
new_snapshot_id = "20251101_12:00:00"
print(current_snapshot_id, new_snapshot_id)
