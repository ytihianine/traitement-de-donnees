from datetime import datetime, timedelta

from scripts.handle_partitions.commun import Actions

# Database
schema = "siep"
tbl_to_keep = ("conso", "bien_information", "facture")
action = Actions.CREATE
dry_run = True

# Si Actions.DROP
drop_from_date = None  # else [datetime.strptime("31/07/2025", "%d/%m/%Y"), ..., datetime.strptime("31/08/2025", "%d/%m/%Y")]


# Si Actions.CREATE
str_date = "01/11/2025"
create_from_date = datetime.strptime(str_date, "%d/%m/%Y")
to_date = create_from_date + timedelta(days=1)
print(create_from_date, to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2025-12-20 23:45:22", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2025-11-01 12:00:00", "%Y-%m-%d %H:%M:%S")
print(curr_import_timestamp, new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20251220_22:55:24"
new_snapshot_id = "20251101_12:00:00"
print(current_snapshot_id, new_snapshot_id)
