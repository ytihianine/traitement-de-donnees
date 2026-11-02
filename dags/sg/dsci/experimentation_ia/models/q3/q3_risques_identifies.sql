-- Dataset: q3.rsiques_identifies
select
    q3_ri.id,
    q3_ri.id_risques_identifies,
    rf.risques,
    q3_ri.no_id,
    q3_ri.import_timestamp,
    q3_ri.snapshot_id,
    q3_ri.snapshot_id_parent
from {{ source('assistant_ia', 'q3_risques_identifies') }} q3_ri
left join {{source('assistant_ia', 'ref_risque')}} rf
    on rf.id = q3_ri.id_risques_identifies
    and rf.import_timestamp = q3_ri.import_timestamp
