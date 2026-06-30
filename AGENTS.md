# AGENTS.md

## Project Map

- `src/dags/` contains the Airflow DAGs.
- Keep pipeline code together in the DAG package: `dags.py`, `process.py`, `tasks.py`, `config.py`, optional `sql/`, `tests/`, `README.md`.
- Shared code belongs in `src/common_tasks/`, `src/infra/`, `src/utils/`, `src/_types/`, and `src/_enums/`.
- `src/constants.py` holds shared defaults, connection IDs, timezone helpers, and the `AIRFLOW_HOME`-based root lookup. If `AIRFLOW_HOME` is unset, it falls back to `/home/onyxia/work/airflow-demo`.

## Commands

- `make setup-dev-env` is the canonical setup path. It creates `env/`, installs `uv`, project dependencies, Airflow, pre-commit, and a git credential helper.
- `make help` lists the available make targets.
- `make init-env-files` generates `.env` files from the example files via `scripts/init_env.bash`.
- `make clean` only removes Python caches and build artefacts.
- Run `pre-commit run --all-files` before finishing. Hooks come from `.pre-commit-config.yaml`: `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-added-large-files`, `black`, and `ruff --fix`.

## DAG Rules

- Follow `docs/convention.md` and `docs/dags.md` for DAG layout and naming.
- Keep parameter validation and notification steps in DAG workflows; do not remove them when extending a pipeline.
- Use `create_dag_params`, `create_default_args`, `DBParams`, and `FeatureFlagsEnable` for DAG configuration.
- `FeatureFlagsEnable` is the repo-standard way to toggle db/mail/s3/file-conversion/Grist behavior without code changes.
- Keep pipeline-specific SQL and processing close to the DAG package unless the code is reused elsewhere.

## Source Of Truth

- If prose conflicts with executable config, trust `Makefile`, `requirements.txt`, and `.pre-commit-config.yaml`.
- For focused verification, prefer running `pytest` against the affected test file or package.
