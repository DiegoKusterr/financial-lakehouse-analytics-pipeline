from __future__ import annotations

import json
import shutil
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DBT_TARGET_DIR = ROOT_DIR / "dbt_finance" / "target"
DOCS_DEMO_DIR = ROOT_DIR / "docs-demo"
DOCS_DEMO_ASSETS_DIR = DOCS_DEMO_DIR / "assets"
DOCS_DEMO_DBT_DIR = DOCS_DEMO_DIR / "dbt-docs"
LINEAGE_GRAPH_SOURCE = ROOT_DIR / "docs" / "assets" / "dbt_lineage_graph.png"
LINEAGE_GRAPH_TARGET = DOCS_DEMO_ASSETS_DIR / "dbt_lineage_graph.png"

DBT_DOCS_FILES = (
    "index.html",
    "manifest.json",
    "catalog.json",
    "run_results.json",
    "graph_summary.json",
    "semantic_manifest.json",
)

PATH_KEYS = {
    "path",
    "original_file_path",
    "compiled_path",
    "patch_path",
    "build_path",
}

PROJECT_DESCRIPTION = (
    "Built from scratch, this project demonstrates a local financial lakehouse pipeline using "
    "PySpark for raw-to-silver ETL, Parquet for storage, and dbt with DuckDB for tested SQL "
    "transformations, lineage and analytics marts."
)

BUTTON_LABEL = "dbt Docs & Technical Demo"

INDEX_HTML = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Financial Lakehouse Analytics Pipeline | Technical Demo</title>
  <meta
    name="description"
    content="Static technical demo for a local financial lakehouse pipeline built with PySpark, Parquet, dbt, DuckDB, and data quality tests."
  >
  <link rel="stylesheet" href="./styles.css">
</head>
<body>
  <main class="shell">
    <section class="hero fade-up">
      <p class="eyebrow">Static Technical Demo</p>
      <div class="hero-grid">
        <div>
          <h1>dbt documentation and lineage for a local financial lakehouse pipeline.</h1>
          <p class="lead">{PROJECT_DESCRIPTION}</p>
          <div class="actions">
            <a class="primary-button" href="./dbt-docs/">{BUTTON_LABEL}</a>
            <span class="inline-note">Static site. No live dashboard. No external services.</span>
          </div>
        </div>
        <aside class="hero-panel">
          <p class="panel-label">Pipeline Summary</p>
          <p class="panel-route">PySpark -&gt; Parquet -&gt; dbt / DuckDB</p>
          <ul class="panel-list">
            <li>Raw CSV generation for reproducible local inputs</li>
            <li>Silver-layer ETL and feature engineering in Spark</li>
            <li>dbt staging, intermediate, and mart layers</li>
            <li>Lineage, source definitions, and tested SQL models</li>
          </ul>
        </aside>
      </div>
    </section>

    <section class="section fade-up">
      <div class="section-heading">
        <p class="eyebrow">What This Demo Shows</p>
        <h2>Public documentation for the validated local pipeline run.</h2>
      </div>
      <div class="cards">
        <article class="card">
          <h3>dbt model documentation</h3>
          <p>Staged, intermediate, and mart models are documented with descriptions, columns, and dependency context.</p>
        </article>
        <article class="card">
          <h3>Model lineage and sources</h3>
          <p>dbt Docs exposes how curated Parquet sources flow into downstream SQL layers and final marts.</p>
        </article>
        <article class="card">
          <h3>Data quality visibility</h3>
          <p>Schema tests cover record grain, null handling, controlled sector values, and referential integrity.</p>
        </article>
      </div>
    </section>

    <section class="section fade-up">
      <div class="section-heading">
        <p class="eyebrow">Architecture</p>
        <h2>Concise explanation of the technical stack.</h2>
      </div>
      <div class="steps">
        <article class="step">
          <span class="step-number">01</span>
          <h3>PySpark raw-to-silver ETL</h3>
          <p>Local Spark transforms raw synthetic CSV files into cleaned silver datasets with schema checks, standardization, deduplication, and window-based feature engineering.</p>
        </article>
        <article class="step">
          <span class="step-number">02</span>
          <h3>Parquet storage</h3>
          <p>Silver outputs are written as Parquet so the pipeline keeps a clear analytical exchange layer between ETL and SQL modeling.</p>
        </article>
        <article class="step">
          <span class="step-number">03</span>
          <h3>dbt staging / intermediate / marts</h3>
          <p>dbt organizes the DuckDB transformation layer into staging entry points, reusable intermediate logic, and analyst-facing marts.</p>
        </article>
        <article class="step">
          <span class="step-number">04</span>
          <h3>DuckDB execution</h3>
          <p>DuckDB runs the local dbt SQL transformations over Parquet-backed sources without requiring a separate warehouse service.</p>
        </article>
        <article class="step">
          <span class="step-number">05</span>
          <h3>Data quality tests</h3>
          <p>dbt tests validate core assumptions before the static docs are published, so the demo represents a tested pipeline rather than a mocked interface.</p>
        </article>
      </div>
    </section>

    <section class="section fade-up">
      <div class="section-heading">
        <p class="eyebrow">Final Marts</p>
        <h2>Three portfolio-facing outputs.</h2>
      </div>
      <div class="marts">
        <article class="mart">
          <h3><code>mart_company_quarterly_metrics</code></h3>
          <p>Company-quarter financial and market metrics for downstream analysis.</p>
        </article>
        <article class="mart">
          <h3><code>mart_sector_financial_health</code></h3>
          <p>Sector-quarter rollups for margin, leverage, returns, and price behavior.</p>
        </article>
        <article class="mart">
          <h3><code>mart_ml_ready_features</code></h3>
          <p>Feature-oriented company-quarter dataset for future analytical or ML experimentation.</p>
        </article>
      </div>
    </section>

    <section class="section fade-up">
      <div class="section-heading">
        <p class="eyebrow">Lineage Snapshot</p>
        <h2>Real dbt lineage from the validated build.</h2>
      </div>
      <figure class="lineage-frame">
        <img
          src="./assets/dbt_lineage_graph.png"
          alt="dbt lineage graph for the financial lakehouse analytics pipeline"
        >
        <figcaption>Source definitions feed staging models, reusable intermediate logic, and final marts documented in dbt Docs.</figcaption>
      </figure>
    </section>
  </main>
</body>
</html>
"""

STYLES_CSS = """\
:root {
  --canvas: #f5f1e8;
  --surface: rgba(255, 252, 247, 0.9);
  --surface-strong: #fffdf8;
  --ink: #17212b;
  --muted: #4b5a68;
  --line: rgba(23, 33, 43, 0.12);
  --accent: #0d6e6e;
  --accent-strong: #0b4f52;
  --accent-soft: rgba(13, 110, 110, 0.12);
  --shadow: 0 22px 48px rgba(23, 33, 43, 0.12);
  --radius-lg: 28px;
  --radius-md: 18px;
  --radius-sm: 12px;
}

* {
  box-sizing: border-box;
}

html {
  scroll-behavior: smooth;
}

body {
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at top left, rgba(13, 110, 110, 0.16), transparent 34%),
    linear-gradient(180deg, #f8f4eb 0%, #f2ede2 100%);
  font-family: "Trebuchet MS", "Gill Sans", sans-serif;
}

img {
  display: block;
  max-width: 100%;
}

code {
  font-family: "Consolas", "Courier New", monospace;
  font-size: 0.95em;
}

.shell {
  width: min(1120px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 2rem 0 4rem;
}

.hero,
.section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow);
  backdrop-filter: blur(12px);
}

.hero {
  padding: 2rem;
}

.hero-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.7fr) minmax(280px, 0.9fr);
  gap: 1.5rem;
  align-items: start;
}

.eyebrow {
  margin: 0 0 0.9rem;
  color: var(--accent-strong);
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.16em;
  text-transform: uppercase;
}

h1,
h2,
h3,
.panel-route {
  font-family: "Georgia", "Times New Roman", serif;
}

h1 {
  margin: 0;
  font-size: clamp(2.35rem, 5vw, 4.5rem);
  line-height: 0.94;
  max-width: 10.5ch;
}

.lead {
  max-width: 58rem;
  margin: 1.2rem 0 0;
  font-size: 1.08rem;
  line-height: 1.7;
  color: var(--muted);
}

.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  align-items: center;
  margin-top: 1.5rem;
}

.primary-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 3.4rem;
  padding: 0 1.4rem;
  border-radius: 999px;
  background: linear-gradient(135deg, var(--accent) 0%, var(--accent-strong) 100%);
  color: #fff;
  font-weight: 700;
  text-decoration: none;
  letter-spacing: 0.01em;
  transition: transform 140ms ease, box-shadow 140ms ease;
  box-shadow: 0 14px 28px rgba(11, 79, 82, 0.24);
}

.primary-button:hover,
.primary-button:focus-visible {
  transform: translateY(-2px);
  box-shadow: 0 20px 34px rgba(11, 79, 82, 0.28);
}

.inline-note {
  color: var(--muted);
  font-size: 0.98rem;
}

.hero-panel {
  padding: 1.15rem;
  border-radius: var(--radius-md);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.86) 0%, rgba(13, 110, 110, 0.08) 100%);
  border: 1px solid rgba(13, 110, 110, 0.14);
}

.panel-label {
  margin: 0;
  color: var(--muted);
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.panel-route {
  margin: 0.7rem 0 1rem;
  font-size: 1.65rem;
  line-height: 1.15;
}

.panel-list {
  margin: 0;
  padding-left: 1.1rem;
  color: var(--muted);
  line-height: 1.7;
}

.section {
  margin-top: 1.4rem;
  padding: 1.6rem;
}

.section-heading {
  margin-bottom: 1.1rem;
}

.section-heading h2 {
  margin: 0;
  font-size: clamp(1.7rem, 3vw, 2.5rem);
  line-height: 1.08;
}

.cards,
.steps,
.marts {
  display: grid;
  gap: 1rem;
}

.cards {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.card,
.step,
.mart {
  padding: 1.2rem;
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: var(--surface-strong);
}

.card h3,
.step h3,
.mart h3 {
  margin: 0 0 0.65rem;
  font-size: 1.15rem;
}

.card p,
.step p,
.mart p,
.lineage-frame figcaption {
  margin: 0;
  color: var(--muted);
  line-height: 1.7;
}

.steps {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.step {
  position: relative;
  overflow: hidden;
}

.step::after {
  content: "";
  position: absolute;
  inset: auto -12% -38% auto;
  width: 9rem;
  height: 9rem;
  border-radius: 50%;
  background: var(--accent-soft);
}

.step-number {
  display: inline-flex;
  margin-bottom: 0.85rem;
  padding: 0.2rem 0.55rem;
  border-radius: 999px;
  background: var(--accent-soft);
  color: var(--accent-strong);
  font-size: 0.8rem;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.marts {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.lineage-frame {
  margin: 0;
  padding: 1rem;
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: #fff;
}

.lineage-frame img {
  border-radius: var(--radius-sm);
}

.lineage-frame figcaption {
  margin-top: 0.9rem;
}

.fade-up {
  animation: fade-up 520ms ease both;
}

@keyframes fade-up {
  from {
    opacity: 0;
    transform: translateY(18px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 900px) {
  .hero-grid,
  .cards,
  .steps,
  .marts {
    grid-template-columns: 1fr;
  }

  h1 {
    max-width: none;
  }
}

@media (max-width: 640px) {
  .shell {
    width: min(100% - 1rem, 100%);
    padding-top: 0.5rem;
  }

  .hero,
  .section {
    padding: 1.2rem;
    border-radius: 22px;
  }

  .actions {
    align-items: stretch;
  }

  .primary-button {
    width: 100%;
  }
}
"""


def normalize_string(value: str, key: str | None = None) -> str:
    normalized = value
    absolute_variants = (
        str(ROOT_DIR),
        str(ROOT_DIR).replace("\\", "/"),
        str(ROOT_DIR).replace("\\", "\\\\"),
    )

    for absolute_path in absolute_variants:
        if absolute_path and absolute_path in normalized:
            normalized = normalized.replace(absolute_path, ".")

    if key in PATH_KEYS or normalized.startswith("dbt_finance://"):
        normalized = normalized.replace("\\", "/")

    return normalized


def sanitize_json_content(value: object, key: str | None = None) -> object:
    if isinstance(value, dict):
        return {dict_key: sanitize_json_content(dict_value, dict_key) for dict_key, dict_value in value.items()}

    if isinstance(value, list):
        return [sanitize_json_content(item, key) for item in value]

    if isinstance(value, str):
        return normalize_string(value, key)

    return value


def ensure_required_inputs() -> None:
    missing = [file_name for file_name in DBT_DOCS_FILES if not (DBT_TARGET_DIR / file_name).exists()]
    if missing:
        raise FileNotFoundError(
            "Missing dbt docs artifacts required for the static demo: " + ", ".join(sorted(missing))
        )

    if not LINEAGE_GRAPH_SOURCE.exists():
        raise FileNotFoundError(f"Missing lineage image at {LINEAGE_GRAPH_SOURCE}")


def reset_docs_demo_dir() -> None:
    if DOCS_DEMO_DIR.exists():
        shutil.rmtree(DOCS_DEMO_DIR)

    DOCS_DEMO_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DEMO_DBT_DIR.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8", newline="\n")


def build_landing_page() -> None:
    write_text(DOCS_DEMO_DIR / "index.html", INDEX_HTML)
    write_text(DOCS_DEMO_DIR / "styles.css", STYLES_CSS)
    write_text(DOCS_DEMO_DIR / ".nojekyll", "")
    shutil.copy2(LINEAGE_GRAPH_SOURCE, LINEAGE_GRAPH_TARGET)


def build_dbt_docs_bundle() -> None:
    for file_name in DBT_DOCS_FILES:
        source_path = DBT_TARGET_DIR / file_name
        target_path = DOCS_DEMO_DBT_DIR / file_name

        if source_path.suffix == ".json":
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            sanitized_payload = sanitize_json_content(payload)
            target_path.write_text(json.dumps(sanitized_payload), encoding="utf-8", newline="\n")
            continue

        html = source_path.read_text(encoding="utf-8")
        html = html.replace("<title>dbt Docs</title>", "<title>dbt Docs | Financial Lakehouse Analytics Pipeline</title>")
        html = html.replace('href="/overview/"', 'href="./"')
        html = html.replace(
            """<link rel="shortcut icon" href="${require('./assets/favicons/favicon.ico')}"/>""",
            "",
        )
        write_text(target_path, html)


def main() -> None:
    ensure_required_inputs()
    reset_docs_demo_dir()
    build_landing_page()
    build_dbt_docs_bundle()
    print(f"Static technical demo generated successfully at {DOCS_DEMO_DIR}")


if __name__ == "__main__":
    main()
