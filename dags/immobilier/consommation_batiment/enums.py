from enum import StrEnum


class Statuts(StrEnum):
    debut_exp = "DEBUT EXP"
    fin_exp = "FIN EXP"
    incomplet = "INCOMPLET"
    complet = "COMPLET"


class TypeEnergie(StrEnum):
    electricite = "électricité"
    gaz = "gaz"
    fioul = "fioul"
    reseau_chaud = "réseau de chaud"
    reseau_froid = "réseau de froid"
