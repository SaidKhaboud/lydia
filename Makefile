.PHONY: install setup_dbt run_pipeline

# Create a virtual environment and install dependencies from pyproject.toml
install:
	uv venv
	uv pip install --upgrade pip
	uv pip install .

# Install dbt packages (like dbt-expectations)
setup_dbt:
	uv run dbt deps --project-dir my_crypto_pipeline/

# Run the entire ELT pipeline using the Prefect flow
run_pipeline:
	uv run python my_pipeline/orchestrate.py