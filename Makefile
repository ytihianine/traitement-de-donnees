
# Variables
PYTHON_VERSION := 3.12
AIRFLOW_VERSION := 3.1.8
ENV_NAME := env

# OS detection
ifeq ($(OS),Windows_NT)
    PYTHON := python
    VENV_BIN := $(ENV_NAME)/Scripts
else
    PYTHON := python3
    VENV_BIN := $(ENV_NAME)/bin
endif

UV_PIP := $(VENV_BIN)/uv pip install --python $(VENV_BIN)/python

.PHONY: help install-sys-packages create-py-env install-airflow install-packages \
        install-pre-commit install-extensions setup-git setup-dev-env init-env-files clean

help: ## Afficher l'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

install-sys-packages: ## Installer les packages système nécessaires (libxml2-dev, libxmlsec1-dev, pkg-config)
	@echo "Ce script va mettre à jour votre système et installer les packages nécessaires."
	@echo "Packages à installer: libxml2-dev libxmlsec1-dev pkg-config"
	@echo ""
	@read -p "Voulez-vous continuer? (o/n): " REPLY; \
	echo ""; \
	if echo "$$REPLY" | grep -qiE '^[oOyY]$$'; then \
		echo "Mise à jour du système en cours..."; \
		sudo apt update; \
		sudo apt install -y libxml2-dev libxmlsec1-dev pkg-config; \
		echo "Système mis à jour avec succès!"; \
	else \
		echo "Mise à jour du système annulée."; \
	fi

create-py-env: ## Créer un nouvel environnement python
	@echo "Création d'un environnement"
	$(PYTHON) -m venv $(ENV_NAME)
	@echo "L'environnement a été créé"
	@echo "Exécuter dans votre terminal: source $(ENV_NAME)/bin/activate"

install-airflow: ## Installer les packages liés à la version d'Airflow
	@echo "Installation des packages Airflow python_version=$(PYTHON_VERSION) & airflow_version=$(AIRFLOW_VERSION)"
	$(UV_PIP) "apache-airflow==$(AIRFLOW_VERSION)" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt"

install-packages: ## Installer les packages python complémentaires
	@echo "Installation de uv"
	$(VENV_BIN)/python -m pip install uv
	$(UV_PIP) -e .
	$(UV_PIP) -r requirements.txt --prerelease=allow
	$(UV_PIP) -r requirements_dev.txt --prerelease=allow

install-pre-commit: ## Installer les pre-commits
	@echo "Installation des pre-commits"
	$(VENV_BIN)/pre-commit install

install-extensions: ## Installer les extensions code-server & les settings
	@echo "Installation des extensions"
	$(VENV_BIN)/python scripts/extensions/install-extensions.py
	@echo "Rechargez votre page pour prendre en compte toutes les modifications"

setup-git: ## Initialiser la configuration git
	@echo "Init git config"
	git config --global credential.helper 'cache --timeout=360000'

setup-dev-env: create-py-env install-packages install-airflow install-pre-commit setup-git ## Installer tout l'environnement de développement

init-env-files: ## Initialiser les fichiers d'environnement des scripts
	@echo "Initialisation des fichiers d'environnement"
	@chmod +x ./scripts/init_env.bash
	@./scripts/init_env.bash
	@echo "Tous les fichiers .env créés à partir de example.env. Veuillez les personnaliser avec vos propres valeurs."

clean: ## Nettoyer les fichiers temporaires Python
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "✓ Nettoyage terminé"
