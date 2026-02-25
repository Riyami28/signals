# Contributing to Signals

## Before You Start

1. Check the open issues before building anything:
   ```bash
   gh issue list --state open --limit 50
   ```
2. Every task has a GitHub issue. If something needs fixing and isn't tracked, open an issue first.
3. Read your assigned issue fully — it contains acceptance criteria, affected files, and dependencies.

## Branch Naming

```
<type>/<issue-number>-<short-description>
```

Examples:
- `fix/3-failing-tests`
- `feat/17-dimension-mapping`
- `chore/10-remove-dead-code`

## Commit Conventions

Use conventional commit format with the issue number:

```
<type>: <description> (#N)
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Examples:
```
fix: replace ? placeholders with %s in db queries (#3)
feat: add dimension column to signal_registry.csv (#17)
```

## Development Workflow

```bash
# 1. Set up (first time only)
make setup

# 2. Create a branch
git checkout -b fix/23-threadpool-exceptions

# 3. Make changes and run tests
make test

# 4. Lint
make lint

# 5. Commit
git add <files>
git commit -m "fix: description (#23)"

# 6. Push and open PR
git push -u origin fix/23-threadpool-exceptions
gh pr create --title "fix: description (#23)"
```

## Code Standards

- **No `SELECT *`** — always list columns explicitly in queries
- **Postgres parameterization** — use `%s` placeholders (not `?`)
- **Test everything** — every issue's acceptance criteria include tests
- **No integrations without rate limiting + retry** — see Epic #13 pattern in CLAUDE.md
- **New signals** — add to `config/signal_registry.csv` with a `dimension` column value

## Testing

```bash
make test                                    # Full suite
pytest tests/test_scoring.py -v             # Single file
pytest -k "test_select_accounts" -v         # By keyword
```

Tests require Postgres running (`make setup` or `docker compose -f docker-compose.local.yml up -d`).

## Pull Request Checklist

- [ ] Issue number referenced in commit messages and PR title
- [ ] All existing tests pass (`make test`)
- [ ] New tests added for the changed logic
- [ ] No `SELECT *` in new queries
- [ ] No hardcoded credentials (use `.env` / `src/settings.py`)
- [ ] CLAUDE.md consulted for patterns and constraints
