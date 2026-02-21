# taxi_rides_ny

dbt project for NYC taxi rides analytics (DuckDB).

## Setup

From the `04-analytics-engineering` directory:

```bash
uv sync
```

## dbt commands

### Creating a new project

These commands are only needed once, when starting a project from scratch (already done for this repo):

```bash
uv run dbt init            # scaffold a new dbt project interactively
uv run dbt debug           # verify your connection & profile configuration
```

### Running this project

Follow these steps in order to get the project up and running:

```bash
uv run dbt deps            # install packages declared in packages.yml
uv run dbt seed            # load seed CSV files into the warehouse
uv run dbt run             # compile & execute all models
uv run dbt test            # run all data tests (schema + singular)
```

`dbt build` combines `seed`, `run`, and `test` in dependency order — handy for a single command:

```bash
uv run dbt build           # seed → run → test in one go
```

If a `build` or `run` partially fails, re-run only the failed nodes:

```bash
uv run dbt retry           # retry the last failed run
```

### Useful options

Run or test a specific model (and optionally its upstream/downstream dependencies):

```bash
uv run dbt run --select fact_trips          # single model
uv run dbt run --select +fact_trips         # model + all upstream deps
uv run dbt test --select stg_green_tripdata # tests for one model
```

### Documentation

Generate and browse the auto-generated project documentation:

```bash
uv run dbt docs generate   # build the docs catalog (target/catalog.json)
uv run dbt docs serve      # start a local web server to browse the docs
```

## SQLFluff linter

The project includes a [SQLFluff](https://sqlfluff.com/) configuration (`.sqlfluff`) using the DuckDB dialect and the dbt templater.

**Lint** (check for issues):

```bash
uv run sqlfluff lint models/
```

**Fix** (auto-fix issues):

```bash
uv run sqlfluff fix models/
```

You can also target a single file:

```bash
uv run sqlfluff lint models/staging/stg_green_tripdata.sql
uv run sqlfluff fix models/staging/stg_green_tripdata.sql
```

> All commands must be run from the `taxi_rides_ny/` directory so SQLFluff picks up the `.sqlfluff` config.

## Resources

- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [SQLFluff documentation](https://docs.sqlfluff.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Community Slack](https://community.getdbt.com/)
