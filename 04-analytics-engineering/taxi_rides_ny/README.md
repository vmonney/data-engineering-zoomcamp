# taxi_rides_ny

dbt project for NYC taxi rides analytics (DuckDB).

## Setup

From the `04-analytics-engineering` directory:

```bash
uv sync
```

## dbt commands

```bash
uv run dbt deps      # install dbt packages
uv run dbt seed      # load seed CSV files
uv run dbt run       # run all models
uv run dbt test      # run tests
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
