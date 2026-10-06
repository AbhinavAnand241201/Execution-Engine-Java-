.PHONY: build test it verify up infra down logs e2e chaos bench

MVNW := ./mvnw -B
E2E_BASE_URL ?= http://localhost:8080

build:  ## Compile and package every module, skipping tests
	$(MVNW) -DskipTests package

test:   ## Unit + architecture tests (no Docker needed)
	$(MVNW) test

it:     ## Integration tests only (needs Docker)
	$(MVNW) verify -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false

verify: ## Full regression: unit + integration (needs Docker)
	$(MVNW) verify

up:     ## Full stack: infrastructure, orchestrator and 3 workers
	docker compose up -d --build --wait

infra:  ## Infrastructure only, for running the apps from the IDE
	docker compose up -d --wait postgres redis kafka wiremock

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

e2e:    ## Black-box tests against a running stack
	$(MVNW) -Pe2e -pl flowforge-e2e -am verify -De2e.baseUrl=$(E2E_BASE_URL)

chaos:  ## Failure scenarios against the running stack; each prints PASS/FAIL
	scripts/chaos/kill-worker.sh
	scripts/chaos/pause-worker.sh
	scripts/chaos/restart-orchestrator.sh
	scripts/chaos/stop-redis.sh

bench:
	@echo "benchmarks are not implemented yet" && exit 1
