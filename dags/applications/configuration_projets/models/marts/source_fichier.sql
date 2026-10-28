WITH latest_import_timestamp as (
    select
        import_timestamp
    from {{ source('conf_projets', 'projet') }} as p
    order by import_timestamp desc
    limit 1
)
select
    d_loc.id_row as id,
    p.projet as nom_projet,
    p.id_direction,
    direction.direction,
    p.id_service,
    service.service,
    split_part(d_loc.location, '/', -1) as nom_source,
    p_loc.fs_folder as s3_key_prefix,
    d_loc.location as file_location
from {{ source('conf_projets', 'projet') }} as p
inner join latest_import_timestamp as latest
    on p.import_timestamp = latest.import_timestamp
left join {{ source('conf_projets', 'projet_location') }} as p_loc
    on p.id_projet = p_loc.id_projet
        and p.import_timestamp = p_loc.import_timestamp
left join {{ source('conf_projets', 'ref_direction') }} as direction
    on p.id_direction = direction.id_direction
        and p.import_timestamp = direction.import_timestamp
left join {{ source('conf_projets', 'ref_service') }} as service
    on p.id_service = service.id_service
        and p.import_timestamp = service.import_timestamp
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
where
    1=1
    and d_loc.stage = 'Source'
    and type_location.type_location = 's3'
