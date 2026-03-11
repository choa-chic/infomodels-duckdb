# Copilot Instructions for `infomodels-duckdb`

## Project Purpose
- This repository validates Common Data Model (CDM) submission files using DuckDB-based data quality (DQ) checks.
- Primary workflow: load model metadata -> create DuckDB tables -> validate files/headers -> load data -> run constraint checks -> summarize/log results.

## Tech Stack and Entry Points
- Language: Python 3.
- Core libraries: `duckdb`, `sqlalchemy`, `pandas`, `PyYAML`, `requests`.
- Main entrypoint: `python -m src.main`.
- Main orchestration lives in `src/main.py`.

## Architecture Overview
- `src/main.py`: End-to-end run coordinator and check sequencing.
- `src/config.py`: global `CONFIG` and `LOGGER` initialization from `config.yml`.
- `src/data_model.py`: loads model schema (service/json), exposes table/field metadata, generates DuckDB DDL.
- `src/load_duckdb.py`: logging schema creation, table creation, CSV/Parquet load functions.
- `src/dq_checks/`: individual check implementations returning `CheckResult` objects.
- `src/dq_checks/check_result.py`: common check result model, status inference, logging, run summary.
- `src/constants.py`: optional tables and threshold definitions.
- `tests/`: pytest suite with data fixtures in `tests/data/`.

## Expected Run Flow (keep this order)
1. Initialize run context and DuckDB logging schema.
2. Load data model (service/json mode).
3. Build skip-lists (optional tables, config-driven skip patterns, missing/invalid tables/columns).
4. Create empty DuckDB tables from model DDL.
5. File completeness checks.
6. Header checks (CSV/Parquet).
7. Load submission files into DuckDB.
8. Run FK, NOT NULL, DISTINCT, and PK checks.
9. Emit final summary through `CheckResult.summary`.

## Coding Conventions for This Repo
- Keep functions small and check-specific; return `CheckResult` from each check function.
- Use existing helpers from `src/util.py` before adding new utility logic.
- Prefer explicit, readable SQL strings; preserve identifier quoting style already in use.
- Keep logging consistent with the existing `LOGGER` patterns (`info`, `debug`, and DQ logs via `CheckResult.log`).
- Preserve lowercase column normalization behavior for file headers.
- Avoid introducing new frameworks or abstractions unless clearly required.

## `CheckResult` Usage Contract
- If status is inferable from violation rate, pass `violation_pct` + `threshold` and allow inference.
- Use explicit `status='SKIPPED'` when prerequisites are missing (table/column/file).
- Always call `result.log(LOGGER, duckdb_conn=...)` before returning.
- Keep check type names stable and descriptive; avoid renaming existing check type strings without migration reason.

## Data Model + DuckDB Rules
- Generate DuckDB DDL via `DataModel.to_duckdb_ddl()`; do not hard-code table schemas.
- Respect `OPTIONAL_TABLES` semantics (not required for completeness checks).
- Preserve current CSV/Parquet branching behavior in `src/main.py`.
- For unsupported branches (e.g., CSV multiple-file-per-table), keep explicit `NotImplementedError` unless fully implemented.

## Testing Guidance
- Use `pytest` and follow current fixture style in `tests/`.
- Favor deterministic tests using local fixtures in `tests/data/`.
- When adding a new check, add/extend targeted tests under `tests/test_dq_checks/`.
- Avoid tests requiring network unless explicitly marked and isolated.

## Safe Change Boundaries
- Prefer surgical changes in existing modules over broad refactors.
- Do not change public function names/signatures unless required by task scope.
- Keep config schema backward compatible where possible.
- If adding config keys, provide defaults and document expected behavior.

## Known Gaps / Caution Areas
- Several test modules are TODO/incomplete (`tests/test_load_duckdb.py`, not-null test scaffold).
- Some logic paths may be fragile; when editing, preserve behavior unless task explicitly requests fixes.
- If addressing bugs, include regression tests near the changed module.

## Preferred Developer Commands
- Install deps: `pip install -r requirements.txt`
- Run app: `python -m src.main`
- Run tests: `pytest -q`

## When Proposing New DQ Checks
- Place check implementation in `src/dq_checks/`.
- Reuse `get_threshold`, `table_exists`, `column_exists`, and `get_table_count` helpers where relevant.
- Integrate execution in `src/main.py` in a phase-consistent location.
- Ensure check outputs include actionable troubleshooting text.