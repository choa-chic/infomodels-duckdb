# Runbook: pointer, then consume

Produce a single DuckDB file containing the whole submission, on a host that does not have
room for the submission and a full copy of it at the same time.

Applies to `access_mode: pointer` with parquet submissions. For csv, see
[When not to use this](#when-not-to-use-this).

---

## Why this exists

A `copy` run loads every submission file into the DuckDB file, so at its peak the host holds
the parquet export *and* a full second copy of the same data. On a measured 3M-row table,
12.6 MB of parquet produced a 19.7 MB DuckDB file -- roughly 2.6x the export on disk at once.

This procedure never holds both in full:

| stage | what is on disk |
|---|---|
| after stage 1 | the submission, plus a DuckDB file holding only views and check results |
| during stage 2 | what is left of the submission, plus what has been written so far |
| after stage 2 | the DuckDB file alone |

Peak becomes the size of the deliverable, which is the floor for this requirement -- you
cannot hold less than the finished file at the moment you finish it.

**The saving comes from `--consume`, not from pointer mode.** Pointer mode alone makes the
*checks* free; if you materialize without consuming, the submission and the full DuckDB file
coexist exactly as they do in `copy` mode.

---

## Before you start

- [ ] Free disk of at least the expected DuckDB size. Budget ~1.5x the parquet export.
- [ ] The submission files can be **regenerated** if lost. Stage 2 deletes them. If a
      re-unload is expensive or the source moves, stop and use `--consume`-free mode instead.
- [ ] The parquet stays readable and in place for the whole of stage 1. The views are live
      handles on those files.
- [ ] `config.yml` has:

```yaml
submission_files:
    dir: /data/infomodels
    file_format: parquet
    multiple_file_per_table: true   # if each table is a directory of parts
    access_mode: pointer
    materialize: off                # stage 2 is run by hand; see step 4
duckdb:
    path: /result/cdm.duckdb
    copy_options: FORMAT PARQUET    # required for parquet; the templates default to csv
```

> `copy_options: FORMAT PARQUET` is easy to miss. The shipped templates default to the csv
> string, which fails immediately on a parquet directory with
> `IO Error: No files found that match the pattern`.

---

## Step 1 -- confirm the config actually took effect

**Do not skip this.** A setting the running build does not read is loaded, printed in the
config dump, and then ignored. A run configured for `pointer` against a build without it
copies the entire submission, and the log shows `access_mode: pointer` a few lines above it
doing so. This has already cost one team a multi-hour run.

Start the run, then check the first lines of the log:

```bash
grep -E "Config loaded from|Ignoring submission_files|Effective settings" "$LOG"
```

Expected:

```
Config loaded from /opt/infomodels/config.yml
Effective settings: config=..., file_format=parquet, multiple_file_per_table=True, access_mode=pointer, duckdb=/result/cdm.duckdb
```

| what you see | what it means |
|---|---|
| `access_mode=pointer` | correct, continue |
| `access_mode=copy` | the config did not take. See [Config did not take effect](#config-did-not-take-effect) |
| `Ignoring submission_files setting(s)...` | that setting has no effect on this run -- read the names |
| `(path taken from INFOMODELS_CONFIG)` | the settings came from that path, not the `config.yml` you edited |
| none of these lines | the build predates this logging. It is not the branch you think it is |

---

## Step 2 -- run the checks

```bash
python3 -m src.main
```

This creates a view per table and runs every conformance check against them. No submission
data is copied.

Confirm the load phase completed:

```bash
grep -c "Pointed " "$LOG"                          # one per table pointed
grep "All submission files prepared" "$LOG"        # only prints if the whole loop succeeded
```

An exit code of `1` means DQ failures were recorded. That is expected here and does not stop
the procedure -- it decides step 3.

---

## Step 3 -- review the results, then decide

```sql
SELECT check_type, table_name, column_name, violation_pct, message
FROM logging.dq
WHERE status = 'FAIL' AND run_id = (SELECT run_id FROM logging.run ORDER BY start_time DESC LIMIT 1);
```

This is the decision point the two-stage split exists for. Deleting the submission is
unrecoverable, and the submission files are what you need to diagnose a failure, so the tool
will not make this call for you.

| situation | do |
|---|---|
| No failures | step 4, plain `--consume` |
| Failures, expected and accepted | step 4 with `--force`, and record why |
| Failures, not understood | **stop.** Fix the source data and re-run stage 1. Do not consume |
| Failures, and you want the file anyway | step 4 without `--consume` -- materializes, keeps the parquet |

---

## Step 4 -- materialize into the deliverable

```bash
python3 -m src.materialize --consume            # clean run
python3 -m src.materialize --consume --force    # accepted failures on record
python3 -m src.materialize                      # materialize only, keep the submission
```

Stage 2 finds the views in the DuckDB file and the submission paths in
`logging.pointer_source`, both written by stage 1. It needs no new settings.

Each table is built and its row count verified **before** its view is dropped, and its
submission file is deleted only after that. A table whose source path was not recorded is
materialized but never deleted.

---

## Step 5 -- verify the deliverable

```sql
-- no views should remain; every CDM relation must be a real table
SELECT table_name, table_type FROM information_schema.tables
WHERE table_schema = 'main' AND table_type = 'VIEW';        -- expect zero rows

-- the file carries its own QA record
SELECT status, count(*) FROM logging.dq GROUP BY status;
```

```bash
# and the file no longer depends on the submission
ls /data/infomodels        # empty after --consume
python3 -c "import duckdb; print(duckdb.connect('/result/cdm.duckdb', read_only=True).execute('SELECT count(*) FROM person').fetchone())"
```

---

## Failure modes

### Config did not take effect

`Effective settings` says `access_mode=copy` while your file says `pointer`.

1. `Config loaded from ...` names the file that was actually read. If it is not the one you
   edited, `INFOMODELS_CONFIG` is set in the environment. Unset it or edit that file.
2. Check `access_mode` is nested **under `submission_files`**, not at the top level or under
   `duckdb`. A key in the wrong place is silently absent -- indentation is enough to do this.
3. Check for the `Ignoring submission_files setting(s)` warning, which names typos.
4. If none of the three startup lines appear at all, the build predates them. Verify the
   checkout: `git log --oneline -1` and `grep -c access_mode src/main.py`.

### The run dies loading the largest tables

Symptom: `Fail to load Parquet to DuckDB: table=measurement` or `table=drug_exposure`, on the
biggest tables while smaller ones succeeded.

This is the `copy`-mode path -- pointer mode logs `Fail to create pointer view` instead. A
failure confined to the largest tables is resource exhaustion, not a format problem: a bad
config fails on the first table, not the two largest. Read the traceback:

- `No space left on device` -- disk. This procedure is the fix.
- `Out of Memory Error` -- set `duckdb.memory_limit`, and check where `temp_directory` points;
  DuckDB spills there and that spill needs disk too.

### A check fails with "file not found" partway through stage 1

Something moved or deleted the parquet while the run was in progress. The views are live
handles, not copies. Re-run stage 1 from the top once the files are back.

### `--consume` refuses to run

```
Run <id> recorded N DQ failure(s). Refusing to delete the submission files.
```

Working as intended. Go back to step 3. Use `--force` only once you have read the failures
and accepted them.

### Stage 2 was interrupted

Safe to re-run. Materializing is idempotent: a table already materialized is detected as a
table and skipped, and its submission file is already gone. Tables not yet reached are still
views with their sources intact.

### Stage 2 says there are no pointer views

Either stage 1 ran in `copy` mode (see the first failure mode), or this DuckDB file has
already been materialized. Check:

```sql
SELECT table_type, count(*) FROM information_schema.tables
WHERE table_schema = 'main' GROUP BY table_type;
```

### The deliverable is bigger than a `copy`-mode file

Expected. DuckDB does not reclaim the space its views occupied, so a file written across two
stages is somewhat larger than one written in a single pass. Compact it once the submission
is gone -- this needs room for both files at once:

```python
import duckdb
con = duckdb.connect()
con.execute("ATTACH '/result/cdm.duckdb' AS s (READ_ONLY); ATTACH '/result/cdm_compact.duckdb' AS d;")
con.execute("COPY FROM DATABASE s TO d;")
```

Do not `CHECKPOINT` after every table in an attempt to control size -- measured worse.

---

## When not to use this

- **csv submissions.** Pointer mode is parquet-only. A DuckDB file is normally *smaller* than
  the csv it came from, so plain `copy` with the files deleted as they load is both simpler
  and better on disk. Re-parsing csv text once per check is also slow -- there is no
  projection pushdown and no statistics.
- **The submission cannot be regenerated.** Use `python3 -m src.materialize` with no
  `--consume`, and accept holding both.
- **You only want to check conformance, not deliver a file.** Stop after stage 2 of this
  runbook. That is pointer mode's cheapest use: run the checks, read `logging.dq`, delete the
  DuckDB file.

---

## One-command alternative

For an automated pipeline that does not stop for human review:

```yaml
submission_files:
    access_mode: pointer
    materialize: consume
    consume_with_dq_failures: false   # true is the equivalent of --force
```

`python3 -m src.main` then does both stages in one process. It still refuses to delete after
DQ failures unless `consume_with_dq_failures` is set. Prefer the two-stage flow whenever a
person is available to read the results first -- that review is the entire point of the split.
