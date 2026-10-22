
select
    contact.id_row,
    p.id_projet,
    p.projet,
    contact.id_contact,
    contact.contact_mail,
    contact.is_mail_generic,
    p.snapshot_id,
    p.snapshot_id_parent,
    p.import_timestamp
from {{ source('conf_projets', 'projet') }} as p
left join {{ source('conf_projets', 'projet_contact') }} as contact
    on p.id_projet = contact.id_projet
        and p.import_timestamp = contact.import_timestamp
order by p.id_projet, contact.id_contact, p.id_row, contact.id_row
