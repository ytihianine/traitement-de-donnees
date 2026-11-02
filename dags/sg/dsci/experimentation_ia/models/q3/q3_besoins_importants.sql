--- Dataset q3.besoins_importants

select
    q3_bp.id,
    q3_bp.id_besoins_prioritaires,
    rb.besoins,
    q3_bp.import_timestamp,
    q3_bp.snapshot_id,
    q3_bp.snapshot_id_parent
from {{ source('assistant_ia', 'q3_besoins_prioritaires') }} q3_bp
left join {{source("assistant_ia", "ref_besoin")}} rb
    on rb.id = q3_bp.id_besoins_prioritaires
    and rb.import_timestamp = q3_bp.import_timestamp
