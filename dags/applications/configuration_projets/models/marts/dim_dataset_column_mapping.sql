
select distinct on (p.id_projet, dataset.dataset, column_mapping.colname_source, column_mapping.import_timestamp)
    column_mapping.id_row,
    p.id_projet,
    p.projet,
    dataset.id_dataset,
    dataset.dataset,
    column_mapping.id_col_mapping,
    column_mapping.colname_source,
    column_mapping.colname_dest,
    column_mapping.to_keep,
    column_mapping.date_archivage,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'dataset') }} as dataset
    on p.id_projet = dataset.id_projet
left join {{ source('conf_projets', 'dataset_column_mapping') }} as column_mapping
    on p.id_projet = column_mapping.id_projet
    and dataset.id_dataset = column_mapping.id_dataset
order by
    p.id_projet,
    dataset.dataset,
    column_mapping.colname_source,
    p.id_row,
    dataset.id_row,
    column_mapping.id_row
