#! /bin/bash

PYTHON_VERSION="3.12"
AIRFLOW_VERSION="3.1.7"

# Pour régler certains problèmes d'installation des packages

echo "Ce script va mettre à jour votre système et installer les packages nécessaires."
echo "Packages à installer: libxml2-dev libxmlsec1-dev pkg-config"
echo ""
read -p "Voulez-vous continuer? (o/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[OoYy]$ ]]; then
    echo "Mise à jour du système en cours..."
    sudo apt update
    sudo apt install -y libxml2-dev libxmlsec1-dev pkg-config
    echo "Système mis à jour avec succès!"
else
    echo "Mise à jour du système annulée."
fi

echo "Installation des packages ..."

# Installation des packages
# Pour travailler avec le projet en mode package
pip install -e ../..
# Être dans les même conditions que l'instance Airflow en cours
pip install "apache-airflow==$AIRFLOW_VERSION" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"
pip install -r requirements.txt

# Installer les pre-commits
pre-commit install

echo Init git config
git config --global credential.helper 'cache --timeout=360000'
