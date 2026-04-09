from enum import Enum


class Statuts(str, Enum):
    debut_exp = "DEBUT EXP"
    fin_exp = "FIN EXP"
    incomplet = "INCOMPLET"
    complet = "COMPLET"


class TypeEnergie(str, Enum):
    electricite = "électricité"
    gaz = "gaz"
    fioul = "fioul"
    reseau_chaud = "réseau de chaud"
    reseau_froid = "réseau de froid"
