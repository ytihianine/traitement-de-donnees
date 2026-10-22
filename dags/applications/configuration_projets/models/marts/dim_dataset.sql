
select
    dataset.id_row,
    p.id_projet,
    p.projet,
    p.id_direction,
    direction.direction,
    p.id_service,
    service.service,
    dataset.id_dataset,
    dataset.dataset,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'dataset') }} as dataset
    on p.id_projet = dataset.id_projet
        and p.import_timestamp = dataset.import_timestamp
left join {{ source('conf_projets', 'ref_direction') }} as direction
    on p.id_direction = direction.id_direction
        and p.import_timestamp = direction.import_timestamp
left join {{ source('conf_projets', 'ref_service') }} as service
    on p.id_service = service.id_service
        and p.import_timestamp = service.import_timestamp
order by p.id_projet, dataset.dataset, p.id_row, dataset.id_row, direction.id_row, service.id_row
