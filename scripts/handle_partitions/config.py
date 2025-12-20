from datetime import datetime, timedelta

from scripts.handle_partitions.commun import Actions

# Database
schema = "siep"
tbl_to_keep = "bien"
action = Actions.DROP
dry_run = True

# Si Actions.CREATE
str_date = "01/11/2025 00:00:00"
from_date = datetime.strptime(str_date, "%d/%m/%Y %H:%M:%S")
to_date = from_date + timedelta(days=1)
print(from_date, to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2025-12-19 17:16:43", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2025-11-01 12:00:00", "%Y-%m-%d %H:%M:%S")
print(curr_import_timestamp, new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20250831_12:00:00"
new_snapshot_id = "20250731_12:00:00"
print(current_snapshot_id, new_snapshot_id)
