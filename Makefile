SHELL := /bin/sh
.DEFAULT_GOAL := help
# Lifecycle targets must stay ordered, including when make is invoked with -j.
.NOTPARALLEL:

COMPOSE ?= docker compose
ARGS ?=
SERVICES ?=
APP = $(COMPOSE) run --rm --no-deps app
NPM = $(COMPOSE) run --rm --no-deps --entrypoint npm app

.PHONY: help check env setup config pull build check-license up demo run ready produce \
	flow-pull inspect sample db shell test-unit test test-acceptance test-all \
	status logs stop down restart reset web

help: ## Show commands and optional arguments (default)
	@awk 'BEGIN { FS = ":.*## "; print "Fiber local demo\n\nUsage: make <target> [ARGS=\"...\"] [SERVICES=\"...\"]\n" } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf '\nExamples:\n  make setup\n  make demo\n  make produce ARGS="--duplicate"\n  make flow-pull ARGS="--claims /workspace/my-claims.json"\n  make logs SERVICES=sink\n  make sample > scenario.json\n'

check: ## Check Docker, Compose, and the running Docker engine
	@command -v docker >/dev/null 2>&1 || { echo 'Docker is required. Install and start Docker Desktop.' >&2; exit 1; }
	@$(COMPOSE) version >/dev/null || { echo 'Docker Compose v2 is required.' >&2; exit 1; }
	@docker info >/dev/null 2>&1 || { echo 'Docker is unavailable. Start Docker Desktop and try again.' >&2; exit 1; }

env: ## Create .env from the example without overwriting existing settings
	@if [ ! -e .env ]; then cp .env.example .env; echo 'Created .env. Review the emulator license and set ACCEPT_EULA=Y before make up.'; fi

setup: env build ## Create local settings and build the Node.js image
	@printf '\nSetup complete. Review the emulator license linked in .env.example,\nset ACCEPT_EULA=Y in .env, then run make demo. Existing settings are preserved.\n'

config: env check ## Validate the Compose configuration without printing credentials
	@$(COMPOSE) config --quiet

pull: check ## Download the emulator, Azurite, and PostgreSQL images
	@$(COMPOSE) pull eventhubs azurite postgres

build: check ## Build the shared Node.js producer/consumer image
	@$(COMPOSE) build sink

check-license: env check
	@$(COMPOSE) config --format json | awk '/"ACCEPT_EULA"[[:space:]]*:[[:space:]]*"Y"/ { accepted = 1 } END { if (!accepted) { print "Review the Microsoft emulator license linked in .env.example, then set ACCEPT_EULA=Y in .env." > "/dev/stderr"; exit 1 } }'

up: check-license build ## Build and start PostgreSQL, Event Hubs, Azurite, and the receiver
	@$(COMPOSE) up -d sink

web: up ## Start both browser demos at http://localhost:58080 (or WEB_HOST_PORT)
	@$(COMPOSE) up -d web
	@$(COMPOSE) port web 8088 | awk '{ print "\nOpen the browser demo at http://" $$0 "\nHumata: /humata   Flow: /flow   Side by side: /" }'

demo: up ## Start services and run the complete send/receive/Flow demo
	@$(APP) demo $(ARGS)

run: demo ## Alias for demo

ready: up ## Start services and wait for authenticated Kafka readiness
	@$(APP) ready $(ARGS)

produce: up ## Publish events; pass --file or --duplicate using ARGS
	@$(APP) produce $(ARGS)

flow-pull: up ## Run Flow's daily-style batch; pass custom inputs using ARGS
	@$(APP) flow-pull $(ARGS)

inspect: build ## Start PostgreSQL and show events, receipts, rejections, and batches
	@$(COMPOSE) up -d --wait postgres
	@$(APP) inspect $(ARGS)

sample: check ## Print fresh example JSON; safe to redirect into a file
	@$(MAKE) --no-print-directory build >&2
	@$(APP) sample $(ARGS)

db: env check ## Start PostgreSQL and open an interactive psql session
	@$(COMPOSE) up -d --wait postgres
	@$(COMPOSE) exec postgres psql -U fiber -d fiber

shell: build ## Open a shell inside the Node.js application image
	@$(COMPOSE) run --rm --no-deps --entrypoint /bin/sh app

test-unit: build ## Run domain-only Node.js tests without starting services
	@$(NPM) run test:unit

test: build ## Start PostgreSQL and run the Node.js/database test suite
	@$(COMPOSE) up -d --wait postgres
	@$(NPM) test

test-acceptance: up ## Test real Kafka traffic, replay, quarantine, and Flow interpretation
	@$(NPM) run test:acceptance

test-all: test test-acceptance ## Run the full suite and the Kafka acceptance exercise

status: check ## Show this demo's containers, including stopped ones
	@$(COMPOSE) ps -a

logs: check ## Follow recent logs; optionally select SERVICES="sink postgres"
	@$(COMPOSE) logs --follow --tail 100 $(if $(strip $(SERVICES)),$(SERVICES),sink web eventhubs postgres)

stop: check ## Stop containers and preserve data; optionally select SERVICES
	@$(COMPOSE) stop $(SERVICES)

down: check ## Remove demo containers and network; preserve PostgreSQL data
	@$(COMPOSE) down

restart: check ## Restart existing containers; optionally select SERVICES=sink
	@$(COMPOSE) restart $(SERVICES)

reset: check ## DELETE this demo's PostgreSQL data and remove its containers
	@$(COMPOSE) down --volumes
