.PHONY: format lint typecheck security test quality

format:
	poetry -C alert_project run ruff format --check .

lint:
	poetry -C alert_project run ruff check .

typecheck:
	poetry -C alert_project run mypy .

security:
	poetry -C alert_project run bandit -r src

test:
	poetry -C alert_project run python -m pytest

quality: format lint typecheck security test