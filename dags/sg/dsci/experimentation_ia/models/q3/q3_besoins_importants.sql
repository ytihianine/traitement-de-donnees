--- Dataset q3.besoins_importants

select
    bp.id,
    rb.id,
    rb.besoins,
    bp.import_timestamp,
    bp.snapshot_id,
    bp.snapshot_id_parent
from q3_besoins_prioritaires bp
left join ref_besoin rb
    on rb.id = bp.id_besoins_prioritaires
    and rb.import_timestamp = bp.import_timestamp
