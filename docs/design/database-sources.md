# Design: reading submissions directly from a database

**Status:** proposal, nothing implemented.
**Goal:** load the CDM straight out of a site's warehouse -- Snowflake, SQL Server, Postgres,
Oracle -- instead of unloading it to parquet or csv first.

---

## Why

Today a submission reaches the tool as files. The site unloads its warehouse to parquet, and
the tool reads those files. The export is pure overhead: it is written, read once, and thrown
away, and while it exists the host holds both it and the DuckDB file being built from it.

`access_mode: pointer` removes the cost of the *checks*. It cannot remove the export itself.

Reading from the database directly does:

| approach | peak local disk |
|---|---|
| `copy` from files | export + full DuckDB file (~2.5x the export) |
| `pointer` + `consume` | the finished DuckDB file (the floor for a file-based source) |
| **direct from database** | **the finished DuckDB file, with no export at all** |

The saving is the whole export. Nothing else on the table comes close, and it removes a
moving part -- an unload that can half-finish, drift from the source, or land parts with
mismatched column orders.

It also broadens the tool: a site with no unload tooling, or no room to stage one, can run
conformance checks by pointing at its warehouse.

---

## What DuckDB can and cannot do

Verified on DuckDB 1.4.0 (`duckdb_extensions()`):

| backend | DuckDB extension | can views read it directly? |
|---|---|---|
| Postgres | `postgres_scanner` | **yes** -- `ATTACH` then view remote tables |
| MySQL | `mysql_scanner` | yes |
| SQLite | `sqlite_scanner` | yes |
| Snowflake | none | no |
| SQL Server | none | no |
| Oracle | none | no |
| (any ODBC) | none | no |

There is no Snowflake, SQL Server, Oracle, or generic ODBC extension. For those three, rows
must be pulled through the Python process in batches. Both paths were validated:

```python
# path A -- attach (postgres/mysql/sqlite only): a view, no local copy
con.execute("ATTACH '...' AS remote (TYPE postgres);")
con.execute("CREATE VIEW person AS SELECT * FROM remote.person;")

# path B -- batches (everything else): stream, insert by explicit column list
for batch in cursor.fetch_arrow_batches():
    con.execute(f'INSERT INTO care_site ({cols}) SELECT {cols} FROM batch')
```

**This asymmetry is the single most important fact in this design.** Pointer mode is
achievable for Postgres and impossible for Snowflake, SQL Server and Oracle. It is not a
matter of effort -- the capability does not exist. The design must state it per backend
rather than pretend uniformity.

---

## How this interacts with `access_mode` and `materialize`

The question that prompted this document. The answer is mostly "it doesn't, and that has to
be enforced," with one genuinely useful combination.

Each source declares two capabilities: `supports_pointer` and `has_removable_source`.

| source | `access_mode: pointer` | `materialize: keep` | `materialize: consume` |
|---|---|---|---|
| parquet files | yes | yes | yes -- deletes the parquet |
| csv files | not implemented | n/a | n/a |
| Postgres / MySQL / SQLite | **yes**, via `ATTACH` | **yes** -- useful | **rejected** |
| Snowflake / SQL Server / Oracle | **rejected** -- no extension exists | n/a | **rejected** |

Three guards, at startup, before any work:

1. **`access_mode: pointer` against a source that cannot be pointed at** raises, naming the
   backend and why. Without this the run silently falls back to copying -- the exact failure
   that cost a multi-hour run and prompted the effective-settings logging.

2. **`materialize: consume` against any database source raises, always.** `consume` means
   "delete the source once its rows are in the DuckDB file." For a file source that deletes
   an export that can be regenerated. For a database source it would mean dropping tables in
   the site's warehouse. There is no configuration under which that is correct, so it must be
   impossible to ask for -- not a warning, not `--force`-able.

3. **`materialize` with `access_mode: copy`** is already rejected today; unchanged.

The one useful combination is **Postgres + pointer + `materialize: keep`**: run every
conformance check against the live warehouse with nothing on local disk, then, if the results
are acceptable, pull one local copy into the deliverable. That is the two-stage flow with the
export removed entirely -- the strongest form of what this tool is trying to do.

---

## Credentials, and a constraint that is easy to miss

`init_duckdb_logging_schema()` writes `str(CONFIG)` verbatim into `logging.run.config` --
inside the DuckDB file. That file is the deliverable and is sent to PEDSnet. Confirmed by
reading a produced file.

Today it holds only paths. The moment a connection string with a password enters `CONFIG`,
that password is shipped to a third party inside the submission.

So, non-negotiably:

- **Credentials never appear in `config.yml`.** It holds a *reference*: `dsn_env: PEDSNET_SRC_DSN`.
  The value is read from the environment at connect time and never stored on `CONFIG`.
- **`logging.run.config` is redacted before insert**, not merely kept clean by convention.
  Anything matching a credential-ish key, plus the userinfo portion of any URL.
- **The `Effective settings` log line redacts too.** That line was added to make runs
  legible; it must not become the thing that leaks a DSN into a log file.

This applies whether or not the rest of this proposal is built, and is worth fixing on its
own merits.

---

## Shape

One interface, several implementations. Everything the check pipeline needs from a submission
today is expressible in five operations:

```python
class Source(Protocol):
    name: str                      # 'parquet', 'snowflake', ... for logs and pointer_source
    supports_pointer: bool
    has_removable_source: bool

    def available_tables(self) -> set[str]:              # missing/extra submission checks
    def columns(self, table: str) -> list[str]:          # header checks; lower-cased
    def reference(self, table: str) -> str:              # path or schema-qualified name
    def load(self, con, table, accept_additional_col=True) -> int
    def create_pointer(self, con, table, accept_additional_col=True) -> None   # raises unless supports_pointer
```

`main.py` stops branching on `file_format` and asks the source instead. The file checks stop
being file checks: "missing submission file" becomes "missing table in the source," which
reads better even for the file case.

Two implementation notes that will otherwise bite:

- **Identifier casing.** Snowflake and Oracle fold to upper case, Postgres to lower. Every
  source lower-cases before the pipeline sees a name, exactly as the parquet path already does.
- **Types.** Do not map vendor types to the data model by hand. Insert into the DDL table and
  let DuckDB cast, which is what both the copy path and the pointer view already do -- so a
  database-sourced table is identical to a parquet-sourced one.

---

## Phasing

Each phase is shippable and independently useful.

**Phase 0 -- extract the interface, change no behavior.** Wrap the existing csv and parquet
paths as `FileSource`. Move the file checks behind `available_tables()` / `columns()`. Proven
by the existing suite passing unchanged plus a source-conformance suite the file sources pass.

**Phase 1 -- generic SQLAlchemy source, copy mode only.** SQLAlchemy is already a dependency.
One implementation reaches Postgres, SQL Server, Oracle and Snowflake through their dialects,
streaming in batches. `supports_pointer = False`, `has_removable_source = False`. Modest
throughput, but it makes every named backend work and pins the guards down.

**Phase 2 -- Arrow fast paths.** Replace the generic cursor with `fetch_arrow_batches()` for
Snowflake, and ADBC where a driver exists. Same interface, materially faster.

**Phase 3 -- attach-based pointer for Postgres/MySQL/SQLite.** Unlocks pointer plus
`materialize: keep`, the combination worth having.

Oracle sits last in each phase: it is the least likely to be needed and the most awkward to
test.

---

## Testing

A single conformance suite every `Source` must pass -- same tables, same expected columns,
same row counts -- run against each implementation. That is what keeps "identical to a
parquet-sourced table" true rather than aspirational.

In CI: file sources, plus SQLite and Postgres (both attachable, both cheap to stand up).
Snowflake, SQL Server and Oracle get the same suite run manually against a real instance,
since credentials cannot live in CI. The guards themselves -- that pointer raises for
Snowflake, that `consume` raises for every database -- are pure unit tests and always run.

---

## Open questions

- **Is the warehouse stable during a run?** Files are a snapshot; a live database is not. A
  run whose FK check sees rows its NOT NULL check did not is a new failure mode with no
  file-based equivalent. Options: require a read-only replica, snapshot into temp tables
  first, or record and accept it. Needs a decision before Phase 1 ships.
- **Where does the row filter live?** Sites rarely want `SELECT *` -- there are date windows
  and cohort restrictions. Config-supplied predicates, or a view the site prepares?
- **Does PEDSnet accept a submission not derived from an unload?** Worth confirming before
  building, since the deliverable's provenance changes.
- **Is `check_extra_submission_file` meaningful here?** A warehouse has many tables that are
  not CDM tables. "Extra" almost certainly means something different against a schema.
