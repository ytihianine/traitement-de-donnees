
select
    d_loc.id_row,
    p.id_projet,
    p.projet,
    dataset.id_dataset,
    dataset.dataset,
    d_loc.stage,
    d_loc.id_type_location,
    type_location.type_location,
    d_loc.location,
    d_loc.id_conn_id,
    connexion.conn_id,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'dataset') }} as dataset
    on p.id_projet = dataset.id_projet
        and p.import_timestamp = dataset.import_timestamp
left join {{ source('conf_projets', 'dataset_location') }} as d_loc
    on p.id_projet = d_loc.id_projet
    and dataset.id_dataset = d_loc.id_dataset
        and dataset.import_timestamp = d_loc.import_timestamp
left join {{ source('conf_projets', 'ref_type_location') }} as type_location
    on d_loc.id_type_location = type_location.id_type_location
        and d_loc.import_timestamp = type_location.import_timestamp
left join {{ source('conf_projets', 'ref_connexion') }} as connexion
    on d_loc.id_conn_id = connexion.id_connexion
        and d_loc.import_timestamp = connexion.import_timestamp
order by
    p.id_projet,
    dataset.dataset,
    d_loc.stage,
    p.id_row,
    dataset.id_row,
    d_loc.id_row,
    type_location.id_row,
    connexion.id_row
