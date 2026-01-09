from enum import Enum


class TypeBinome(Enum):
    sans_binome = "Sans binome"
    avec_binome = "Binome"


class Raison(Enum):
    sans_nom = "Nom pas renseigné dans les données sources"
    sans_categorie = "Catégorie pas renseignée dans les données sources"
    sans_localisation = "Localisation pas renseignée dans les données sources"
    sans_mentor = "Sans mentor"
    sans_mentore = "Sans mentoré"


class ColName(str, Enum):
    nom = "A_NOM"
    prenom = "B_PRENOM"
    mail = "Mail"
    age = "J_AGE1"
    # Il s'agit du statut RH (titulaire ou contractuel)
    statut = "C_STATUT"
    categorie = "E_CATEGORIE"
    direction = "F_DIRECTION"
    perimetre = "G_AFFECTATION"
    dir_grp = "DIR_GRP"
    departement = "H2_Departement"
    ville = "H1_VILLE"
    # Il s'agit du statut dans le programme MERCI (Mentor ou Mentoré)
    merci_statut = "QA_STATUT"
