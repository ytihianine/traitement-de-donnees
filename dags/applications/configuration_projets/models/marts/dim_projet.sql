
select
    p.id_row,
    p.id_projet,
    p.projet,
    p.id_direction,
    direction.direction,
    p.id_service,
    service.service,
    p_loc.bucket,
    p_loc.fs_folder,
    p_loc.fs_folder_tmp,
    p_loc.db_schema,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'projet_location') }} as p_loc
    on p.id_projet = p_loc.id_projet
        and p.import_timestamp = p_loc.import_timestamp
left join {{ source('conf_projets', 'ref_direction') }} as direction
    on p.id_direction = direction.id_direction
        and p.import_timestamp = direction.import_timestamp
left join {{ source('conf_projets', 'ref_service') }} as service
    on p.id_service = service.id_service
        and p.import_timestamp = service.import_timestamp
order by p.id_projet, p.id_row, p_loc.id_row, direction.id_row, service.id_row
