.PHONY: setup test test-unit test-integration test-chaos load-test lint format typecheck build up down logs restart teardown clean help

setup:
	python3 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements-dev.txt
	cp .env.example .env

test:
	PYTHONPATH=. pytest tests/ --cov=. --cov-report=term-missing

test-unit:
	pytest tests/test_aggregator.py tests/test_anomaly.py -v

test-integration:
	pytest tests/test_consumers.py -v --timeout=60

test-chaos:
	pytest tests/test_fault_tolerance.py -v

load-test:
	locust -f tests/load_test.py --headless -u 1000 -r 100 --run-time 60s

lint:
	flake8 . --max-line-length=100 --exclude=venv,__pycache__

format:
	black . --line-length=100

typecheck:
	mypy . --config-file=pyproject.toml

build:
	docker compose -f docker/docker-compose.yml build

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

logs:
	docker compose -f docker/docker-compose.yml logs -f

restart: down up

teardown:
	docker compose -f docker/docker-compose.yml down -v --remove-orphans

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -f .coverage

help:
	@printf '%-20s %s\n' 'Target' 'Description'
	@printf '%-20s %s\n' '-----' '-----------'
	@printf '%-20s %s\n' 'setup' 'Create a virtualenv and install dev dependencies'
	@printf '%-20s %s\n' 'test' 'Run the full test suite with coverage'
	@printf '%-20s %s\n' 'test-unit' 'Run unit tests for aggregator and anomaly detection'
	@printf '%-20s %s\n' 'test-integration' 'Run integration tests with a timeout'
	@printf '%-20s %s\n' 'test-chaos' 'Run fault-tolerance and chaos tests'
	@printf '%-20s %s\n' 'load-test' 'Run a headless Locust load test'
	@printf '%-20s %s\n' 'lint' 'Run flake8 for linting'
	@printf '%-20s %s\n' 'format' 'Format the codebase with black'
	@printf '%-20s %s\n' 'typecheck' 'Run mypy with the repository config'
	@printf '%-20s %s\n' 'build' 'Build the Docker images'
	@printf '%-20s %s\n' 'up' 'Start the docker-compose stack'
	@printf '%-20s %s\n' 'down' 'Stop the docker-compose stack'
	@printf '%-20s %s\n' 'logs' 'Follow container logs'
	@printf '%-20s %s\n' 'restart' 'Restart the docker-compose stack'
	@printf '%-20s %s\n' 'teardown' 'Remove the stack and volumes'
	@printf '%-20s %s\n' 'clean' 'Remove Python caches and coverage output'
