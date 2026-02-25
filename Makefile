.PHONY: setup dev test lint clean migrate

# Full bootstrap: venv, deps, Docker, DB schema, seed data
setup:
	./scripts/bootstrap.sh

# Start the web UI (localhost:8788)
dev:
	./signals ui --port 8788

# Run all tests
test:
	@source .venv/bin/activate 2>/dev/null || true; \
	pytest -q --tb=short

# Run linter + format check
lint:
	@source .venv/bin/activate 2>/dev/null || true; \
	ruff check . && ruff format --check .

# Apply pending DB migrations
migrate:
	@source .venv/bin/activate 2>/dev/null || true; \
	python -m src.main migrate

# Stop Docker services and remove build artifacts
clean:
	docker compose -f docker-compose.local.yml down 2>/dev/null || true
	rm -rf .venv __pycache__ **/__pycache__ *.egg-info
