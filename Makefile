.PHONY: setup test lint build up down clean

setup:
	python3 -m venv venv
	./venv/bin/pip install -r requirements.txt
	cp .env.example .env

test:
	export PYTHONPATH=. && pytest tests/

lint:
	flake8 .
	black --check .
	mypy .

build:
	docker compose -f docker/docker-compose.yml build

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
