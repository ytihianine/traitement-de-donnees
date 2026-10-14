
select distinct on (p.id_projet, dataset.dataset, dataset_location.stage)
    dataset_location.id_row,
    p.id_projet,
    p.projet,
    dataset.id_dataset,
    dataset.dataset,
    dataset_location.stage,
    dataset_location.id_type_location,
    type_location.type_location,
    dataset_location.location,
    dataset_location.id_conn_id,
    connexion.conn_id,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'dataset') }} as dataset
    on p.id_projet = dataset.id_projet
left join {{ source('conf_projets', 'dataset_location') }} as dataset_location
    on p.id_projet = dataset_location.id_projet
    and dataset.id_dataset = dataset_location.id_dataset
left join {{ source('conf_projets', 'ref_type_location') }} as type_location
    on dataset_location.id_type_location = type_location.id_type_location
left join {{ source('conf_projets', 'ref_connexion') }} as connexion
    on dataset_location.id_conn_id = connexion.id_connexion
order by
    p.id_projet,
    dataset.dataset,
    dataset_location.stage,
    p.id_row,
    dataset.id_row,
    dataset_location.id_row,
    type_location.id_row,
    connexion.id_row
