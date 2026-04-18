from datetime import datetime, timedelta

from src.utils.config.vars import custom_logger
from scripts.handle_partitions.commun import Actions

# Database
schema = "siep"
tbl_to_keep = ("bien",)
action = Actions.UPDATE_SNAPSHOT
dry_run = False

# Si Actions.DROP
drop_from_date = None
# else
# drop_from_date = [
#     datetime.strptime("31/07/2025", "%d/%m/%Y"),
#   datetime.strptime("31/07/2025", "%d/%m/%Y"),
#   ...,
#   datetime.strptime("31/08/2025", "%d/%m/%Y")
#   ]


# Si Actions.CREATE
str_date = "01/11/2025"
create_from_date = datetime.strptime(str_date, "%d/%m/%Y")
to_date = create_from_date + timedelta(days=1)
custom_logger.info(msg=create_from_date)
custom_logger.info(msg=to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2026-03-13 16:04:55", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2025-02-01 12:00:00", "%Y-%m-%d %H:%M:%S")
custom_logger.info(msg=curr_import_timestamp)
custom_logger.info(msg=new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20260313_16:04:55"
new_snapshot_id = "20250201_12:00:00"
custom_logger.info(msg=current_snapshot_id)
custom_logger.info(msg=new_snapshot_id)
