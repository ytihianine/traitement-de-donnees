
select
    p.id_row,
    p.id_projet,
    p.projet,
    p.id_direction,
    direction.direction,
    p.id_service,
    service.service,
    location.bucket,
    location.fs_folder,
    location.fs_folder_tmp,
    location.db_schema,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'projet_location') }} as location
    on p.id_projet = location.id_projet
left join {{ source('conf_projets', 'ref_direction') }} as direction
    on p.id_direction = direction.id_direction
left join {{ source('conf_projets', 'ref_service') }} as service
    on p.id_service = service.id_service
order by p.id_projet, p.id_row, location.id_row, direction.id_row, service.id_row
