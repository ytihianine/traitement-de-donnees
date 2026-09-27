from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

execution_options = {
    "grist_doc": ExecutionOptions(),
    # Référentiels questionnaire 1
    "ref_q1_direction": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q5_domaine": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q6_niveau_utilisation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q9_cas_usage": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Référentiels questionnaire 2
    "ref_q28_raisons_perte": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q25_impact_observe": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q24_impact_identifie": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q23_taux_correction": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q22_typologie_erreurs": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q20_autres_ia": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q16_taches": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q14_evolution_craintes": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q13_facteurs_progression": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q10_principaux_freins": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q6_participation_programme": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q5_formation_suivie": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q3_niveau_2": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q7_accords": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Référentiels questionnaire 2 bis
    "ref_raisons_non_utilisation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Référentiels questionnaire 3
    "ref_q6_formation_suivie": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q7_particip_programme": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q8_raisons_non_participation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q11_leviers_progressions": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q12_impacts_taches_pro": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q14_taches_rebarbativ": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q17_autres_outils": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q17_satisfaction_autre_outil": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q18_comparaisons": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q19_fonctionnalites": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q21_risques_identifies": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "ref_q25_besoins": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Répartition par entité
    "quota_par_entite": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Suivi des expérimentateurs
    "experimentateurs": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Questionnaire 1
    "questionnaire_1": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_1_cas_usage": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_1_besoins_accompagnement": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire 2
    "questionnaire_2": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_2_typologie_interaction": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_facteurs_progression": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_formation_suivie": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_freins": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False),
    "questionnaire_2_impact_identifie": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_impact_observe": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_participation": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_taches": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False),
    "questionnaire_2_typologie_erreurs": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire_2_bis
    "questionnaire_2_bis": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_2_bis_raisons_non_utilisation": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire_3
    "questionnaire_3": ExecutionOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_3_formation_suivie": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_programme_rdv": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_leviers_progression": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_fonctionnalites": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_risques_identifies": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_besoins_prioritaires": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_besoins_moindres": ExecutionOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
}
