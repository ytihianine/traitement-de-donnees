
select
    -- Demandes
    dmd."id",
    dmd."id_direction",
    dmd."id_type_demande",
    dmd."id_agent",
    dmd."objet",
    dmd."statut",
    dmd."description",
    dmd."niveau_de_complexite",
    dmd."charge_estimee",
    dmd."charge_consommee",
    dmd."ecart_de_charge",
    dmd."mail_contact",
    dmd."date_creation",
    dmd."date_modification",
    dmd.import_timestamp,
    dmd.snapshot_id,
    dmd.snapshot_id_parent,
    -- Agent
    equipe."agent",
    -- Ref direction
    ref_dir."direction",
    -- Ref type demande
    ref_type."type_demande"
from {{ source('demo', 'demande') }} as dmd
left join {{ source('demo', 'equipe') }} as equipe
    on dmd.import_timestamp = equipe.import_timestamp
    and dmd.id_agent = equipe.id
left join {{ source('demo', 'ref_direction') }} as ref_dir
    on dmd.import_timestamp = ref_dir.import_timestamp
    and dmd.id_direction = ref_dir.id
left join {{ source('demo', 'ref_type_demande') }} as ref_type
    on dmd.import_timestamp = ref_type.import_timestamp
    and dmd.id_type_demande = ref_type.id
order by
	dmd.import_timestamp,
	dmd."date_creation",
    dmd."id_direction",
    dmd."id_type_demande",
    dmd."id_agent",
    dmd."id"
