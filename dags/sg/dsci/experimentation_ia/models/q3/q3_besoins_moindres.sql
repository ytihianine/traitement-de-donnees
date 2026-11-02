-- Dataset: q3.besoins_moindres
select
    q3_bm.id,
    q3_bm.id_besoins_moindres,
    rb.besoins,
    q3_bm.no_id,
    q3_bm.import_timestamp,
    q3_bm.snapshot_id,
    q3_bm.snapshot_id_parent
from {{ source('assistant_ia', 'q3_besoins_moindres') }} q3_bm
left join {{source('assistant_ia', 'ref_besoin')}} rb
    on rb.id = q3_bm.id_besoins_moindres
    and rb.import_timestamp = q3_bm.import_timestamp
