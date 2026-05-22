DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.aip (
    id SERIAL,
    aip_mail TEXT,
    aip_balf_mail TEXT,
    aip_direction_geree TEXT,
    structure TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.certificat (
    id SERIAL,
    id_certificat BIGINT,
    dn TEXT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    status  TEXT,
    date_revocation DATE,
    certif_dir_profile TEXT,
    certif_dir_dn TEXT,
    certif_dir_subjectid TEXT,
    certif_dir_contact TEXT,
    certif_dir_mail TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    etat TEXT,
    version TEXT,
    version_serveur TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.certificat_contact (
    id SERIAL,
    id_certificat BIGINT,
    contact TEXT,
    agent_direction TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.historique_certificat (
    id SERIAL,
    agent_mail TEXT,
    cn TEXT,
    agent_structure TEXT,
    date_fin_validite DATE,
    date_debut_validite DATE,
    agent_direction TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.mandataire (
    id SERIAL,
    libelle TEXT,
    sigle TEXT,
    mail TEXT,
    structure TEXT,
    date DATE,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE MATERIALIZED VIEW certificat_igc.historique_certificat_rank_mvw AS 
select
  id,
  agent_mail,
  agent_direction,
  date_debut_validite,
    row_number() over (
      partition by agent_mail, DATE_TRUNC('month', date_debut_validite), import_date
      order by date_debut_validite asc
    ) as monthly_rank,
    row_number() over (
      partition by agent_mail, import_date
      order by
        case when agent_direction is not null then 0 else 1 end asc,
        date_debut_validite desc
    ) as latest_rank,
  import_date
from
	certificat_igc.historique_certificat
;


-- Tables de datavisualisation
-- Dernière version
with cte_certificat_contact as (
-- Classer les lignes pour conserver un seul contact par id_certificat pour la suite
  select distinct on (id_certificat, import_date)
    id_certificat,
    contact,
    agent_direction,
    import_date
  from certificat_igc.certificat_contact
  where contact is not null
  order by id_certificat, import_date, contact
),
cte_best_match as (
select
	igc_cert.id_certificat as id_certificat,
	cert_histo_rank.id as histo_id,
	row_number() over (
      partition by igc_cert.id_certificat, igc_cert.import_date
order by
	ABS(igc_cert.date_debut_validite - cert_histo_rank.date_debut_validite) asc nulls last
    ) as proximity_rank,
  igc_cert.import_date
from
	certificat_igc.certificat igc_cert
left join cte_certificat_contact cte_cc
  on
	igc_cert.id_certificat = cte_cc.id_certificat
  and igc_cert.import_date = cte_cc.import_date
left join certificat_igc.historique_certificat_rank_mvw cert_histo_rank
        on
	cert_histo_rank.agent_mail = cte_cc.contact
	and DATE_TRUNC('month', cert_histo_rank.date_debut_validite) between
            DATE_TRUNC('month', igc_cert.date_debut_validite) - interval '1 month'
            and DATE_TRUNC('month', igc_cert.date_debut_validite) + interval '1 month'
		and cert_histo_rank.monthly_rank = 1
    and cert_histo_rank.import_date = igc_cert.import_date
)
  select
  igc_cert.id_certificat,
  igc_cert.dn,
  igc_cert.subjectid,
  igc_cert.contact,
  igc_cert.email,
  igc_cert.date_debut_validite,
  igc_cert.date_fin_validite,
  igc_cert.profile,
  igc_cert.status,
  igc_cert.date_revocation,
  igc_cert.certif_dir_profile,
  igc_cert.certif_dir_dn,
  igc_cert.certif_dir_subjectid,
  igc_cert.certif_dir_contact,
  igc_cert.certif_dir_mail,
  igc_cert.ac,
  igc_cert.type_offre,
  igc_cert.supports,
  igc_cert.etat,
  igc_cert.version,
  igc_cert.version_serveur,
  igc_cert.import_timestamp,
  igc_cert.import_date,
  igc_cert.snapshot_id,
  igc_histo.id as histo_id,
	igc_histo.agent_direction as histo_agent_direction,
	cte_bm.histo_id as bm_histo_id,
	coalesce(
      igc_cert.certif_dir_profile,
      igc_cert.certif_dir_dn,
      igc_cert.certif_dir_subjectid,
      igc_cert.certif_dir_contact,
      igc_cert.certif_dir_mail,
      igc_histo.agent_direction,
      cte_cc.agent_direction,
      cert_histo_latest_rank.agent_direction,
      'ABSENT'
  ) as certificat_direction
from
	certificat_igc.certificat igc_cert
left join cte_certificat_contact cte_cc
  on
	igc_cert.id_certificat = cte_cc.id_certificat
  and cte_cc.import_date = igc_cert.import_date
left join cte_best_match cte_bm
    on
	cte_bm.id_certificat = igc_cert.id_certificat
	and cte_bm.proximity_rank = 1
  and cte_bm.import_date = igc_cert.import_date
left join certificat_igc.historique_certificat igc_histo
    on
	igc_histo.id = cte_bm.histo_id
left join certificat_igc.historique_certificat_rank_mvw cert_histo_latest_rank
    on
  cert_histo_latest_rank.agent_mail = cte_cc.contact
  and cert_histo_latest_rank.latest_rank = 1
  and cert_histo_latest_rank.import_date = igc_cert.import_date
;
