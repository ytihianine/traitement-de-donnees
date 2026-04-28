from datetime import datetime, timedelta

from src.constants import custom_logger
from scripts.handle_partitions.commun import Actions

# Database
schema = "siep"
tbl_to_keep = ("conso_", "facture_", "bien_information_complementaire")
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
str_date = "01/03/2026"
create_from_date = datetime.strptime(str_date, "%d/%m/%Y")
to_date = create_from_date + timedelta(days=1)
custom_logger.info(msg=create_from_date)
custom_logger.info(msg=to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2026-04-27 18:45:00", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2026-03-01 13:00:00", "%Y-%m-%d %H:%M:%S")
custom_logger.info(msg=curr_import_timestamp)
custom_logger.info(msg=new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20260427_12:15:00"
new_snapshot_id = "20260301_12:00:00"
custom_logger.info(msg=current_snapshot_id)
custom_logger.info(msg=new_snapshot_id)
