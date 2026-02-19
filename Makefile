
# Variables
PYTHON_VERSION=3.12
AIRFLOW_VERSION=3.1.7
ENV_NAME = env

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
	python -m venv $(ENV_NAME)
	@echo "L'environnement a été créé"
	@echo "Activation du nouvel environnement"
	@echo "Exécuter dans votre terminal: source $(ENV_NAME)/bin/activate"


upgrade-pip: ## Mettre à jour pip
	@echo Mise à jour de pip
	$(ENV_NAME)/bin/pip install --upgrade pip

install-airflow: ## Installer les packages liés à la version d'Airflow
	@echo "Installation des packages Airflow python_version=$(PYTHON_VERSION) & airflow_version=$(AIRFLOW_VERSION)"
	$(ENV_NAME)/bin/pip install "apache-airflow==$(AIRFLOW_VERSION)" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt"

install-packages: ## Installer les packages python complémentaires
	$(ENV_NAME)/bin/pip install -e .
	$(ENV_NAME)/bin/pip install -r requirements.txt
	$(ENV_NAME)/bin/pip install -r requirements_dev.txt

install-pre-commit: ## Installer les pre-commits
	@echo "Installation des pre-commits"
	$(ENV_NAME)/bin/pre-commit install

setup-git:
	@echo "Init git config"
	git config --global credential.helper 'cache --timeout=360000'

setup-dev-env: create-py-env upgrade-pip install-packages install-airflow install-pre-commit setup-git ## Installer tout l'environnement de développement


# Nettoyage
clean: ## Nettoie les fichiers temporaires Python
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "✓ Nettoyage terminé"
