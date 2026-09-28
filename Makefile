SHELL := /bin/bash

.PHONY: help env-setup up down logs proto test-go test-py lint

help:
	@echo "DocuMind - dev commands"
	@echo ""
	@echo "  make env-setup   Copy all .env.example -> .env"
	@echo "  make up          Start infrastructure (docker compose)"
	@echo "  make down        Stop infrastructure"
	@echo "  make logs        Follow logs of all services"
	@echo "  make proto       Generate Go/Python code from proto"
	@echo "  make test-go     Run Go tests"
	@echo "  make test-py     Run Python tests"
	@echo "  make lint        Run linters"

env-setup:
	@echo "Setting up .env files..."
	@test -f infra/.env || cp infra/.env.example infra/.env
	@test -f go/.env || cp go/.env.example go/.env
	@test -f python/ai_worker/.env || cp python/ai_worker/.env.example python/ai_worker/.env
	@echo "Done. Edit the files if you need custom values."

up:
	cd infra && docker compose up -d

down:
	cd infra && docker compose down

logs:
	cd infra && docker compose logs -f

proto:
	cd go && buf generate

test-go:
	cd go && go test ./...

test-py:
	cd python/ai_worker && pytest

lint:
	cd go && golangci-lint run ./...
	cd python/ai_worker && ruff check .