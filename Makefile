.PHONY: install run kill

# Create a virtual environment and install dependencies from pyproject.toml
install:
	pip install uv
	uv venv
	uv pip install --upgrade pip
	uv pip install .

run:
	docker compose up

kill:
	docker compose down -v
