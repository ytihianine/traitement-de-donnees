
select distinct on (p.id_projet, documentation.type_documentation)
    documentation.id_row,
    p.id_projet,
    p.projet,
    documentation.id_documentation,
    documentation.type_documentation,
    documentation.lien,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'projet_documentation') }} as documentation
    on p.id_projet = documentation.id_projet
order by p.id_projet, documentation.type_documentation, p.id_row, documentation.id_row
