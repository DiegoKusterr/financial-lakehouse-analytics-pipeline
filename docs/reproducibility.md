# Reproducibility and validation

`requirements.txt` applies `constraints.txt`; `requirements-dev.txt` adds pytest under the same constraints. No dependency upgrade is required for the local fixes. Direct packages and the dbt integration stack are pinned to the already installed environment used for local validation:

| Component | Version |
| --- | --- |
| CPython | 3.11.9 |
| Temurin JDK | 17.0.16+8 |
| pandas / NumPy | 2.2.3 / 2.3.4 |
| PyArrow | 18.1.0 |
| PySpark / bundled Hadoop | 3.5.8 / 3.3.4 |
| DuckDB | 1.5.4 |
| dbt Core / dbt-duckdb | 1.11.12 / 1.10.1 |
| pytest | 8.4.2 |

The previously audited published CI manifest identifies dbt Core 1.12.0; the committed historical Docs identify 1.11.12. The historical CI run did not retain a complete dependency lock available to this audit, so its other resolved versions cannot be asserted. The new constraints use the locally available, tested stack rather than guessing historical versions or installing an unverified upgrade. Actions now records `pip freeze` with its test evidence to remove this ambiguity for future runs.

These constraints are not a full transitive lock with hashes. Other transitive dependencies and the Ubuntu runner image can still change. The constrained stack passed a fresh installation and complete execution on Ubuntu in the [reviewed PR validation run](https://github.com/DiegoKusterr/financial-lakehouse-analytics-pipeline/actions/runs/43005147062). Python packages such as NumPy 2 are used for standalone generation, not Spark's optional ML or pandas-on-Spark APIs.

## Validation without replacing existing data

The local correction was validated in a new temporary workspace containing copied source code and dbt project files, with newly generated synthetic CSV files. Original data directories were not used for execution. The output validator supports an explicit workspace:

```powershell
python src/validate_outputs.py --root C:\Path\To\isolated-workspace
```

It scans all Parquet columns, rejects empty outputs and duplicate/NULL keys, queries actual DuckDB marts, compares them with gold Parquet, and derives expected cardinalities from distinct raw business keys. Thus the default generator reconciles 50 companies, 26,100 daily rows, 400 company-quarter rows and 48 sector-quarter rows without rejecting valid alternative input sizes.

Regression tests use actual production Spark functions and render the actual SQL models into an in-memory DuckDB connection. No test-only replacement transformation is used. The deployment workflow runs this suite before the existing ETL, dbt tests, validation and Docs steps. Test results are saved before Docs generation overwrites `run_results.json`.

Local verification on 2026-11-11 completed with **54 Python regression cases passed**, **61 executed dbt tests passed**, and all **9 dbt models built successfully**. Synthetic generation, native Spark Parquet writes, the strengthened validator, Docs generation and the static demo builder all ran successfully in the isolated Windows workspace. The ML mart retained 400 feature rows: 50 unknown terminal labels and 350 training-eligible rows. All 4 missing gross-profit values and 6 missing cash-flow values remained NULL.

The initial installed dependency metadata closure was also checked against both Windows and Linux environment markers with no missing/conflicting requirements. No package was installed/upgraded during local verification.

## Linux PR verification

On 2026-11-11, Ubuntu CI used CPython 3.11.18 and Temurin 17.0.21+9. A fresh install honored all 15 constraints. The actual logs and saved test artifact confirmed **54 Python cases passed**, **9 models built**, **61 dbt tests passed**, native Spark ETL succeeded, and the strengthened validator reconciled every output. Pages deployment was correctly skipped for the Pull Request.

The reviewed generated Docs contain the corrected SQL, 9 models, 3 sources and 61 tests. The tracked `docs-demo/dbt-docs/` snapshot was refreshed from this successful Linux run. GitHub Pages independently rebuilds Docs from source after successful validation on each push to `main`; generated timestamps and invocation identifiers therefore differ from the tracked snapshot. `run_results.json` describes Docs generation; executed test results remain in the separate `pipeline-validation` Actions artifact.
