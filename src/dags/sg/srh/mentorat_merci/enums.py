from enum import Enum


class ChoixDirection(Enum):
    SANS_PREF = "pas de préférence"
    MEME_DIR = "Même direction"
    AUTRE_DIR = "Une autre direction"


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
