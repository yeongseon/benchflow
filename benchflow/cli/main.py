from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from importlib import import_module
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit, urlunsplit

import typer
import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import benchflow.workers.python.psycopg_worker  # noqa: F401, E402  # pyright: ignore[reportUnusedImport]
import benchflow.workers.python.sqlalchemy_worker  # noqa: F401, E402  # pyright: ignore[reportUnusedImport]

# Optional workers — loaded when their driver packages are installed
try:
    import benchflow.workers.python.pycubrid_worker  # noqa: F401, E402  # pyright: ignore[reportUnusedImport]
except ImportError:
    pass

try:
    import benchflow.workers.python.cubriddb_worker  # noqa: F401, E402  # pyright: ignore[reportUnusedImport]
except ImportError:
    pass

try:
    import benchflow.workers.python.pymysql_worker  # noqa: F401, E402  # pyright: ignore[reportUnusedImport]
except ImportError:
    pass
from benchflow.core.metrics.aggregator import bootstrap_ratio_ci
from benchflow.core.result import CompareResult, ComparisonItem, RunResult
from benchflow.core.scenario.schema import Scenario
from benchflow.workers.protocol import get_worker_factory

app = typer.Typer(
    name="bench",
    help="BenchForge \u2014 Scenario-based polyglot database benchmark platform",
    no_args_is_help=True,
)
console = Console()

_WORKER_IMPORT_MAP: dict[str, str] = {
    "python+psycopg": "benchflow.workers.python.psycopg_worker",
    "python+sqlalchemy": "benchflow.workers.python.sqlalchemy_worker",
    "python+pymysql": "benchflow.workers.python.pymysql_worker",
    "python+pycubrid": "benchflow.workers.python.pycubrid_worker",
    "python+cubriddb": "benchflow.workers.python.cubriddb_worker",
}

_DB_PRESETS: dict[str, dict[str, str]] = {
    "postgresql": {
        "label": "PostgreSQL",
        "stack_id": "python+psycopg",
        "driver": "psycopg",
        "dsn_template": "postgres://user:pass@host:5432/db",
    },
    "mysql": {
        "label": "MySQL",
        "stack_id": "python+pymysql",
        "driver": "pymysql",
        "dsn_template": "mysql://user:pass@host:3306/db",
    },
    "cubrid": {
        "label": "CUBRID",
        "stack_id": "python+pycubrid",
        "driver": "pycubrid",
        "dsn_template": "cubrid://user:pass@host:33000/db",
    },
}


@app.command()
def run(
    scenario_path: Annotated[str, typer.Argument(help="Path to scenario YAML file")],
    output: Annotated[str, typer.Option("--output", "-o", help="Output JSON path")] = "",
    iterations: Annotated[
        int | None,
        typer.Option(
            "--iterations",
            "-n",
            help="Override iteration count",
        ),
    ] = None,
    seed: Annotated[
        int | None,
        typer.Option(
            "--seed",
            help="Random seed for reproducibility (overrides scenario)",
        ),
    ] = None,
    capture_db_info: Annotated[
        bool,
        typer.Option(
            "--capture-db-info",
            help="Capture DB server config via introspect()",
        ),
    ] = False,
    target_filter: Annotated[
        list[str] | None,
        typer.Option("--target", "-t", help="Run only specific targets (by name, repeatable)"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Validate scenario and show execution plan only"),
    ] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Run a benchmark scenario against all defined targets."""
    if verbose:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    from benchflow.cli.progress import RichRunProgress
    from benchflow.core.runner.runner import run_benchmark
    from benchflow.core.scenario.loader import load_scenario

    scenario = load_scenario(scenario_path)

    all_target_names = [t.name for t in scenario.targets]
    if not scenario.targets:
        console.print("[red]Error:[/red] No targets defined in scenario file.")
        console.print("Add a 'targets' section to your scenario YAML.")
        raise typer.Exit(1)

    if target_filter:
        selected = set(target_filter)
        scenario.targets = [target for target in scenario.targets if target.name in selected]
        if not scenario.targets:
            console.print("[red]Error:[/red] No targets matched --target filters.")
            console.print(f"Requested: {sorted(selected)}")
            console.print(f"Available: {all_target_names}")
            raise typer.Exit(1)

    n_iter = iterations if iterations is not None else None
    effective_iterations = n_iter or scenario.experiment.iterations
    run_id_override: str | None = None
    output_path = output
    if not output_path:
        run_id_override = str(uuid.uuid4())[:8]
        output_path = f"reports/{run_id_override}.json"

    console.print(f"[bold]Running scenario:[/bold] {scenario.name}")
    console.print(
        f"  concurrency={scenario.load.concurrency}, "
        f"duration={scenario.load.duration}s, "
        f"warmup={scenario.load.warmup.duration}s"
    )
    if effective_iterations > 1:
        console.print(
            f"  iterations={effective_iterations}, seed={seed or scenario.experiment.seed}"
        )
    console.print(
        "  selected targets: "
        f"{[f'{target.name} ({target.stack_id})' for target in scenario.targets]}"
    )
    if target_filter:
        console.print(f"  target filter: {sorted(set(target_filter))}")
    if dry_run:
        console.print("  mode: dry-run")
    console.print(f"[dim]Results will be saved to:[/dim] {output_path}")
    console.print()

    if dry_run:
        console.print("[yellow]Dry run:[/yellow] benchmark execution skipped.")
        console.print(f"Dry run complete. Use 'bench run {scenario_path}' to execute.")
        return

    result = run_benchmark(
        scenario,
        iterations_override=n_iter,
        seed_override=seed,
        capture_db_info=capture_db_info,
        progress=RichRunProgress(console),
        run_id_override=run_id_override,
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    result.save(output_path)

    _print_summary(result)
    console.print(f"\n[green]Results saved to:[/green] {output_path}")


@app.command()
def compare(
    baseline_path: Annotated[str, typer.Argument(help="Path to baseline result JSON")],
    contender_path: Annotated[str, typer.Argument(help="Path to contender result JSON")],
    output: Annotated[str, typer.Option("--output", "-o", help="Output comparison JSON")] = "",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print comparison result as JSON to stdout"),
    ] = False,
) -> None:
    """Compare two benchmark results."""
    baseline = RunResult.load(baseline_path)
    contender = RunResult.load(contender_path)

    scenario_match = baseline.scenario.signature == contender.scenario.signature
    if not scenario_match:
        console.print(
            "[yellow]Warning:[/yellow] Scenario signatures differ. "
            "Results may not be directly comparable."
        )

    comparisons: list[ComparisonItem] = []
    throughput_values: dict[tuple[str, str], tuple[float, float]] = {}

    baseline_targets = {t.stack_id: t for t in baseline.targets}
    contender_targets = {t.stack_id: t for t in contender.targets}

    baseline_iter_metrics = _collect_iteration_metrics(baseline)
    contender_iter_metrics = _collect_iteration_metrics(contender)

    common_stacks = set(baseline_targets.keys()) & set(contender_targets.keys())

    for stack_id in sorted(common_stacks):
        bt = baseline_targets[stack_id]
        ct = contender_targets[stack_id]

        baseline_steps = {s.name: s for s in bt.steps}
        contender_steps = {s.name: s for s in ct.steps}

        common_steps = set(baseline_steps.keys()) & set(contender_steps.keys())

        for step_name in sorted(common_steps):
            bs = baseline_steps[step_name]
            cs = contender_steps[step_name]
            throughput_values[(stack_id, step_name)] = (bs.throughput_ops_s, cs.throughput_ops_s)

            ratio_ci = None
            significant = None
            b_p50s = baseline_iter_metrics.get((stack_id, step_name, "p50_ns"), [])
            c_p50s = contender_iter_metrics.get((stack_id, step_name, "p50_ns"), [])
            if len(b_p50s) >= 2 and len(c_p50s) >= 2:
                ratio_ci, significant = bootstrap_ratio_ci(b_p50s, c_p50s)

            comparisons.append(
                ComparisonItem(
                    stack_id=stack_id,
                    step=step_name,
                    baseline=bs.latency_summary,
                    contender=cs.latency_summary,
                    p50_ratio=round(cs.latency_summary.p50_ns / bs.latency_summary.p50_ns, 3)
                    if bs.latency_summary.p50_ns > 0
                    else 0.0,
                    p95_ratio=round(cs.latency_summary.p95_ns / bs.latency_summary.p95_ns, 3)
                    if bs.latency_summary.p95_ns > 0
                    else 0.0,
                    p99_ratio=round(cs.latency_summary.p99_ns / bs.latency_summary.p99_ns, 3)
                    if bs.latency_summary.p99_ns > 0
                    else 0.0,
                    throughput_ratio=round(cs.throughput_ops_s / bs.throughput_ops_s, 3)
                    if bs.throughput_ops_s > 0
                    else 0.0,
                    error_delta=cs.errors - bs.errors,
                    ratio_ci=ratio_ci,
                    significant=significant,
                )
            )

    compare_result = CompareResult(
        baseline_run_id=baseline.run_id,
        contender_run_id=contender.run_id,
        scenario_name=baseline.scenario.name,
        scenario_match=scenario_match,
        comparisons=comparisons,
    )

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as f:
            json.dump(compare_result.model_dump(), f, indent=2, default=str)

    if json_output:
        typer.echo(json.dumps(compare_result.model_dump(mode="json"), indent=2))
        return

    _print_comparison(compare_result, throughput_values)


@app.command()
def report(
    result_path: Annotated[str, typer.Argument(help="Path to result JSON")],
    output: Annotated[str, typer.Option("--output", "-o", help="Output HTML path")] = "",
) -> None:
    """Generate an HTML report from benchmark results."""
    from benchflow.core.report.html import generate_html_report

    result = RunResult.load(result_path)

    if not output:
        output = result_path.replace(".json", ".html")

    html = generate_html_report(result)
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        f.write(html)

    console.print(f"[green]Report generated:[/green] {output}")


@app.command("init")
def init_scenario(
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output scenario YAML path"),
    ] = "scenario.yaml",
) -> None:
    console.print("[bold cyan]BenchForge scenario wizard[/bold cyan]")

    scenario_name = typer.prompt("Scenario name", default="my-scenario").strip() or "my-scenario"
    db_input = typer.prompt(
        "Databases (comma-separated: postgresql, mysql, cubrid, custom)",
        default="postgresql",
    )
    selected_dbs = _parse_database_choices(db_input)
    if not selected_dbs:
        console.print("[red]No valid database choice selected.[/red]")
        raise typer.Exit(1)

    targets: list[dict[str, Any]] = []
    for db_key in selected_dbs:
        if db_key == "custom":
            targets.extend(_prompt_custom_targets())
            continue

        preset = _DB_PRESETS[db_key]
        default_target_name = f"{db_key}-{preset['driver']}"
        dsn = typer.prompt(
            f"{preset['label']} DSN",
            default=preset["dsn_template"],
        )
        targets.append(
            {
                "name": default_target_name,
                "stack_id": preset["stack_id"],
                "driver": preset["driver"],
                "dsn": dsn,
            }
        )

    benchmark_type = typer.prompt(
        "Benchmark type (point_select, mixed_crud, full_scan, custom)",
        default="point_select",
    ).strip()
    scenario_body = _benchmark_template(benchmark_type)
    if scenario_body is None:
        console.print(f"[red]Unknown benchmark type:[/red] {benchmark_type}")
        raise typer.Exit(1)

    concurrency = typer.prompt("Concurrency", default=4, type=int)
    duration = typer.prompt("Duration seconds", default=10, type=int)
    warmup = typer.prompt("Warmup seconds", default=5, type=int)
    iterations = typer.prompt("Iterations", default=3, type=int)

    scenario_dict: dict[str, Any] = {
        "name": scenario_name,
        "description": f"Generated by bench init ({benchmark_type})",
        "steps": scenario_body["steps"],
        "load": {
            "concurrency": concurrency,
            "duration": duration,
            "warmup": {"duration": warmup},
        },
        "experiment": {
            "iterations": iterations,
            "seed": None,
            "pause_between": 2.0,
        },
        "targets": targets,
    }
    if "setup" in scenario_body:
        scenario_dict["setup"] = scenario_body["setup"]
    if "teardown" in scenario_body:
        scenario_dict["teardown"] = scenario_body["teardown"]

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_render_scenario_yaml(scenario_dict), encoding="utf-8")

    console.print(f"[green]Scenario generated:[/green] {out_path}")
    console.print(f"Next: [cyan]bench validate {out_path}[/cyan]")
    console.print(f"Then: [cyan]bench run {out_path}[/cyan]")


@app.command()
def validate(
    scenario_path: Annotated[str, typer.Argument(help="Path to scenario YAML file")],
) -> None:
    checks: list[tuple[str, bool, str]] = []
    validation_errors: list[str] = []
    parsed_data: Any = None
    scenario: Scenario | None = None
    path = Path(scenario_path)

    if not path.exists():
        console.print(
            Panel(
                f"✗ File not found: {path}",
                title="Validation",
                border_style="red",
            )
        )
        raise typer.Exit(1)

    try:
        with open(path) as f:
            parsed_data = yaml.safe_load(f)
        if parsed_data is None:
            raise ValueError("Scenario YAML is empty")
        checks.append(("YAML parsed", True, "YAML syntax looks good"))
    except yaml.YAMLError as exc:
        checks.append(("YAML parsed", False, f"Invalid YAML syntax: {exc}"))
    except ValueError as exc:
        checks.append(("YAML parsed", False, str(exc)))

    if checks and checks[-1][1]:
        try:
            scenario = Scenario.model_validate(parsed_data)
            checks.append(("Schema validation", True, "Scenario fields are valid"))
        except ValidationError as exc:
            checks.append(("Schema validation", False, "Scenario schema validation failed"))
            validation_errors = _friendly_validation_errors(exc)

    if scenario is not None:
        missing_workers: list[str] = []
        for target in scenario.targets:
            if not _ensure_worker_registered(target.stack_id):
                missing_workers.append(target.stack_id)

        if missing_workers:
            unique_missing = sorted(set(missing_workers))
            checks.append(
                (
                    "Worker registry",
                    False,
                    "No registered worker for stack_id(s): " + ", ".join(unique_missing),
                )
            )
        else:
            checks.append(("Worker registry", True, "All target stack_ids have workers"))

    _print_validation_panel(checks)

    failures = [check for check in checks if not check[1]]
    if failures:
        if validation_errors:
            console.print("[bold red]Friendly validation errors[/bold red]")
            for err in validation_errors:
                console.print(f"  • {err}")
        raise typer.Exit(1)

    assert scenario is not None
    _print_validated_scenario_summary(scenario)


@app.command()
def show(
    path: Annotated[str, typer.Argument(help="Path to scenario YAML or result JSON")],
) -> None:
    file_path = Path(path)
    if not file_path.exists():
        console.print(f"[red]File not found:[/red] {file_path}")
        raise typer.Exit(1)

    kind = _detect_show_file_type(file_path)
    if kind == "scenario":
        from benchflow.core.scenario.loader import load_scenario

        try:
            scenario = load_scenario(file_path)
        except (ValueError, ValidationError, yaml.YAMLError) as exc:
            console.print(f"[red]Unable to parse scenario:[/red] {exc}")
            raise typer.Exit(1)
        _print_scenario_details(scenario)
        return

    try:
        result = RunResult.load(str(file_path))
    except (ValueError, json.JSONDecodeError, ValidationError) as exc:
        console.print(f"[red]Unable to parse result JSON:[/red] {exc}")
        raise typer.Exit(1)

    console.print(
        Panel(
            f"Run: [bold]{result.run_id}[/bold]\n"
            f"Scenario: [bold]{result.scenario.name}[/bold]\n"
            f"Created: {result.created_at}",
            title="Result Summary",
            border_style="cyan",
        )
    )
    _print_summary(result)


def _parse_database_choices(raw: str) -> list[str]:
    alias_map = {
        "postgres": "postgresql",
        "postgresql": "postgresql",
        "mysql": "mysql",
        "cubrid": "cubrid",
        "custom": "custom",
    }
    choices: list[str] = []
    for token in raw.split(","):
        cleaned = token.strip().lower()
        mapped = alias_map.get(cleaned)
        if mapped and mapped not in choices:
            choices.append(mapped)
    return choices


def _prompt_custom_targets() -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    while True:
        index = len(targets) + 1
        name = typer.prompt("Custom target name", default=f"custom-{index}")
        stack_id = typer.prompt("Custom stack_id", default="python+custom")
        driver = typer.prompt("Custom driver", default="custom")
        dsn = typer.prompt("Custom DSN", default="driver://user:pass@host:1234/db")
        targets.append(
            {
                "name": name.strip() or f"custom-{index}",
                "stack_id": stack_id.strip() or "python+custom",
                "driver": driver.strip() or "custom",
                "dsn": dsn.strip() or "driver://user:pass@host:1234/db",
            }
        )
        if not typer.confirm("Add another custom target?", default=False):
            break
    return targets


def _benchmark_template(benchmark_type: str) -> dict[str, Any] | None:
    normalized = benchmark_type.strip().lower()
    if normalized == "point_select":
        return {
            "steps": [
                {
                    "name": "point-select",
                    "query": "SELECT id, username, email FROM users WHERE id = %(id)s",
                    "params": {"id": "random_int(1, 100000)"},
                }
            ]
        }
    if normalized == "mixed_crud":
        return {
            "setup": {
                "queries": [
                    "CREATE TABLE IF NOT EXISTS crud_items "
                    "(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
                ]
            },
            "teardown": {"queries": ["DROP TABLE IF EXISTS crud_items"]},
            "steps": [
                {
                    "name": "insert-item",
                    "query": "INSERT INTO crud_items (id, value) VALUES (%(id)s, %(value)s)",
                    "params": {
                        "id": "random_int(1, 100000)",
                        "value": "random_int(1, 1000)",
                    },
                },
                {
                    "name": "select-item",
                    "query": "SELECT id, value FROM crud_items WHERE id = %(id)s",
                    "params": {"id": "random_int(1, 100000)"},
                },
                {
                    "name": "update-item",
                    "query": "UPDATE crud_items SET value = %(value)s WHERE id = %(id)s",
                    "params": {
                        "id": "random_int(1, 100000)",
                        "value": "random_int(1, 1000)",
                    },
                },
            ],
        }
    if normalized == "full_scan":
        return {
            "steps": [
                {
                    "name": "full-scan",
                    "query": "SELECT * FROM users",
                }
            ]
        }
    if normalized == "custom":
        step_count = typer.prompt("How many custom steps?", default=1, type=int)
        steps: list[dict[str, Any]] = []
        for index in range(max(step_count, 1)):
            step_number = index + 1
            step_name = typer.prompt(
                f"Step {step_number} name",
                default=f"custom-step-{step_number}",
            )
            query = typer.prompt(f"Step {step_number} SQL query")
            steps.append(
                {
                    "name": step_name.strip() or f"custom-step-{step_number}",
                    "query": query,
                }
            )
        return {"steps": steps}
    return None


def _yaml_quote(value: str) -> str:
    return json.dumps(value)


def _render_scenario_yaml(scenario_dict: dict[str, Any]) -> str:
    lines: list[str] = [
        "# BenchForge scenario generated by `bench init`",
        "# Validate with: bench validate <path>",
        "# Run with:      bench run <path>",
        "",
        "# Scenario metadata",
        f"name: {_yaml_quote(str(scenario_dict['name']))}",
        f"description: {_yaml_quote(str(scenario_dict.get('description') or ''))}",
        "",
    ]

    setup = scenario_dict.get("setup")
    if isinstance(setup, dict):
        lines.extend(["# Optional setup queries", "setup:", "  queries:"])
        for query in setup.get("queries", []):
            lines.append(f"    - {_yaml_quote(str(query))}")
        lines.append("")

    teardown = scenario_dict.get("teardown")
    if isinstance(teardown, dict):
        lines.extend(["# Optional teardown queries", "teardown:", "  queries:"])
        for query in teardown.get("queries", []):
            lines.append(f"    - {_yaml_quote(str(query))}")
        lines.append("")

    lines.extend(["# Benchmark steps", "steps:"])
    for step in scenario_dict["steps"]:
        lines.append(f"  - name: {_yaml_quote(str(step['name']))}")
        lines.append(f"    query: {_yaml_quote(str(step['query']))}")
        params = step.get("params")
        if isinstance(params, dict) and params:
            lines.append("    params:")
            for key, value in params.items():
                lines.append(f"      {key}: {_yaml_quote(str(value))}")
    lines.append("")

    load = scenario_dict["load"]
    lines.extend(
        [
            "# Load profile",
            "load:",
            f"  concurrency: {load['concurrency']}",
            f"  duration: {load['duration']}",
            "  warmup:",
            f"    duration: {load['warmup']['duration']}",
            "",
        ]
    )

    experiment = scenario_dict["experiment"]
    seed_line = "null" if experiment.get("seed") is None else str(experiment["seed"])
    lines.extend(
        [
            "# Experiment settings",
            "experiment:",
            f"  iterations: {experiment['iterations']}",
            f"  seed: {seed_line}",
            f"  pause_between: {experiment['pause_between']}",
            "",
            "# Database targets",
            "targets:",
        ]
    )
    for target in scenario_dict["targets"]:
        lines.extend(
            [
                f"  - name: {_yaml_quote(str(target['name']))}",
                f"    stack_id: {_yaml_quote(str(target['stack_id']))}",
                f"    driver: {_yaml_quote(str(target['driver']))}",
                f"    dsn: {_yaml_quote(str(target['dsn']))}",
            ]
        )

    return "\n".join(lines) + "\n"


def _friendly_validation_errors(exc: ValidationError) -> list[str]:
    messages: list[str] = []
    for err in exc.errors():
        loc_parts = [str(part) for part in err.get("loc", ())]
        field_name = ".".join(loc_parts) if loc_parts else "field"
        message = str(err.get("msg", "Invalid value"))
        if message == "Field required":
            messages.append(f"Missing required field '{field_name}'")
            continue
        normalized = message[0].lower() + message[1:] if message else "invalid value"
        messages.append(f"Field '{field_name}': {normalized}")
    return messages


def _ensure_worker_registered(stack_id: str) -> bool:
    try:
        get_worker_factory(stack_id)
        return True
    except KeyError:
        module_name = _WORKER_IMPORT_MAP.get(stack_id)
        if module_name is None:
            return False
    try:
        import_module(module_name)
        get_worker_factory(stack_id)
        return True
    except (ImportError, KeyError):
        return False


def _print_validation_panel(checks: list[tuple[str, bool, str]]) -> None:
    lines = [f"{'✓' if ok else '✗'} {name}: {detail}" for name, ok, detail in checks]
    has_failure = any(not ok for _, ok, _ in checks)
    title = "Validation failed" if has_failure else "Validation passed"
    border = "red" if has_failure else "green"
    console.print(Panel("\n".join(lines), title=title, border_style=border))


def _print_validated_scenario_summary(scenario: Scenario) -> None:
    target_ids = ", ".join(target.stack_id for target in scenario.targets) or "(none)"
    summary = (
        f"Name: [bold]{scenario.name}[/bold]\n"
        f"Steps: {len(scenario.steps)}\n"
        f"Targets: {target_ids}\n"
        f"Concurrency: {scenario.load.concurrency}\n"
        f"Duration: {scenario.load.duration}s\n"
        f"Iterations: {scenario.experiment.iterations}"
    )
    console.print(Panel(summary, title="Scenario Summary", border_style="cyan"))


def _detect_show_file_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return "scenario"
    if suffix == ".json":
        return "result"
    content = path.read_text(encoding="utf-8").strip()
    return "result" if content.startswith("{") else "scenario"


def _print_scenario_details(scenario: Scenario) -> None:
    description = scenario.description or "(none)"
    console.print(
        Panel(
            f"Name: [bold]{scenario.name}[/bold]\nDescription: {description}",
            title="Scenario",
            border_style="cyan",
        )
    )

    steps_table = Table(title="Steps")
    steps_table.add_column("Name", style="cyan")
    steps_table.add_column("Query Preview", style="white")
    for step in scenario.steps:
        preview = step.query.strip().replace("\n", " ")
        if len(preview) > 60:
            preview = preview[:57] + "..."
        steps_table.add_row(step.name, preview)
    console.print(steps_table)

    load_panel = (
        f"Concurrency: {scenario.load.concurrency}\n"
        f"Duration: {scenario.load.duration}s\n"
        f"Warmup: {scenario.load.warmup.duration}s"
    )
    console.print(Panel(load_panel, title="Load", border_style="green"))

    experiment_panel = (
        f"Iterations: {scenario.experiment.iterations}\n"
        f"Seed: {scenario.experiment.seed}\n"
        f"Pause Between: {scenario.experiment.pause_between}s"
    )
    console.print(Panel(experiment_panel, title="Experiment", border_style="magenta"))

    targets_table = Table(title="Targets")
    targets_table.add_column("Name", style="cyan")
    targets_table.add_column("Stack", style="white")
    targets_table.add_column("DSN", style="yellow")
    for target in scenario.targets:
        targets_table.add_row(target.name, target.stack_id, _redact_dsn(target.dsn))
    console.print(targets_table)


def _redact_dsn(dsn: str) -> str:
    parts = urlsplit(dsn)
    if parts.password is None:
        return dsn
    user = parts.username or ""
    auth = f"{user}:***" if user else "***"
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    netloc = f"{auth}@{host}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _print_summary(result: RunResult) -> None:
    console.print(f"[bold]Benchmark Results — {result.scenario.name}[/bold]")

    step_rankings: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for target in result.targets:
        table = Table()
        table.add_column("Step", style="white")
        table.add_column("Ops", justify="right")
        table.add_column("p50 (ms)", justify="right", style="green")
        table.add_column("p95 (ms)", justify="right", style="yellow")
        table.add_column("p99 (ms)", justify="right", style="red")
        table.add_column("Throughput", justify="right")

        if target.status != "ok":
            table.add_row("[red]Target failed[/red]", "-", "-", "-", "-", "-")
        elif not target.steps:
            table.add_row("[dim]No steps executed[/dim]", "-", "-", "-", "-", "-")
        else:
            for step in target.steps:
                ls = step.latency_summary
                table.add_row(
                    step.name,
                    f"{step.ops:,}",
                    f"{ls.p50_ns / 1_000_000:.2f}",
                    f"{ls.p95_ns / 1_000_000:.2f}",
                    f"{ls.p99_ns / 1_000_000:.2f}",
                    f"{step.throughput_ops_s:,.0f} ops/s",
                )
                if ls.p95_ns > 0:
                    step_rankings[step.name].append((target.stack_id, ls.p95_ns))

        console.print(Panel(table, title=target.stack_id, border_style="cyan"))

    _print_cv_warnings(result)

    console.print("\n[bold]🏆 Summary[/bold]")
    if len(result.targets) <= 1:
        console.print("  [dim]Single target run; no cross-target comparison needed.[/dim]")
        return

    if not step_rankings:
        console.print("  [dim]No comparable p95 latency data available.[/dim]")
        return

    for step_name in sorted(step_rankings):
        ranked = sorted(step_rankings[step_name], key=lambda item: item[1])
        if len(ranked) < 2:
            console.print(
                f"  {step_name} → [dim]insufficient comparable targets for winner selection[/dim]"
            )
            continue

        winner, winner_p95 = ranked[0]
        runner_up_p95 = ranked[1][1]
        speedup = runner_up_p95 / winner_p95 if winner_p95 > 0 else 0.0
        others = ", ".join(stack_id for stack_id, _ in ranked[1:])
        console.print(
            f"  {step_name} → [green]{winner} wins ({speedup:.2f}x faster, p95)[/green]"
            f" [dim]vs {others}[/dim]"
        )


_CV_WARN_THRESHOLD = 0.20
_CV_CRITICAL_THRESHOLD = 0.50


def _print_cv_warnings(result: RunResult) -> None:
    if not result.aggregate:
        return

    warnings: list[str] = []
    for agg_target in result.aggregate:
        for agg_step in agg_target.steps:
            cv = agg_step.p50_ns.cv
            if cv >= _CV_CRITICAL_THRESHOLD:
                warnings.append(
                    f"[red]⚠ {agg_target.stack_id}/{agg_step.step_name}: "
                    f"CV={cv:.0%} — results unreliable, check for system noise[/red]"
                )
            elif cv >= _CV_WARN_THRESHOLD:
                warnings.append(
                    f"[yellow]⚠ {agg_target.stack_id}/{agg_step.step_name}: "
                    f"CV={cv:.0%} — consider more iterations[/yellow]"
                )

    if warnings:
        console.print()
        for w in warnings:
            console.print(f"  {w}")


def _print_comparison(
    compare: CompareResult,
    throughput_values: dict[tuple[str, str], tuple[float, float]] | None = None,
) -> None:
    table = Table(title="Comparison: baseline vs contender")
    table.add_column("Stack / Step", style="cyan")
    table.add_column("Metric", style="white")
    table.add_column("Baseline", justify="right")
    table.add_column("Contender", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Sig.", justify="center")

    throughput_values = throughput_values or {}

    for comparison in compare.comparisons:
        baseline_throughput, contender_throughput = throughput_values.get(
            (comparison.stack_id, comparison.step),
            (0.0, 0.0),
        )

        sig_label = _format_significance(comparison)

        rows = [
            (
                "p50",
                f"{comparison.baseline.p50_ns / 1_000_000:.2f} ms",
                f"{comparison.contender.p50_ns / 1_000_000:.2f} ms",
                _format_change(comparison.p50_ratio, higher_is_better=False),
            ),
            (
                "p95",
                f"{comparison.baseline.p95_ns / 1_000_000:.2f} ms",
                f"{comparison.contender.p95_ns / 1_000_000:.2f} ms",
                _format_change(comparison.p95_ratio, higher_is_better=False),
            ),
            (
                "p99",
                f"{comparison.baseline.p99_ns / 1_000_000:.2f} ms",
                f"{comparison.contender.p99_ns / 1_000_000:.2f} ms",
                _format_change(comparison.p99_ratio, higher_is_better=False),
            ),
            (
                "throughput",
                f"{baseline_throughput:,.0f} ops/s",
                f"{contender_throughput:,.0f} ops/s",
                _format_change(comparison.throughput_ratio, higher_is_better=True),
            ),
        ]

        for index, (metric, baseline_value, contender_value, change) in enumerate(rows):
            stack_step = f"{comparison.stack_id}\n{comparison.step}" if index == 0 else ""
            sig_cell = sig_label if index == 0 else ""
            table.add_row(
                stack_step, metric, baseline_value, contender_value, change, sig_cell
            )

    console.print(table)
    console.print(f"\nBaseline: {compare.baseline_run_id} → Contender: {compare.contender_run_id}")
    if not compare.scenario_match:
        console.print("[yellow]⚠ Scenario signatures differ[/yellow]")


def _format_significance(item: ComparisonItem) -> str:
    if item.ratio_ci is None or item.significant is None:
        return "[dim]—[/dim]"
    ci = item.ratio_ci
    ci_text = f"[{ci.low:.2f}, {ci.high:.2f}]"
    if item.significant:
        return f"[bold green]✓[/bold green] {ci_text}"
    return f"[dim]✗[/dim] {ci_text}"


def _format_change(ratio: float, higher_is_better: bool) -> str:
    if ratio == 0.0:
        return "N/A"
    improved = ratio >= 1.0 if higher_is_better else ratio <= 1.0
    if higher_is_better:
        arrow = "↑" if ratio >= 1.0 else "↓"
    else:
        arrow = "↓" if ratio <= 1.0 else "↑"
    color = "green" if improved else "red"
    return f"[{color}]{arrow} {ratio:.2f}x[/{color}]"


def _collect_iteration_metrics(
    result: RunResult,
) -> dict[tuple[str, str, str], list[float]]:
    """Map (stack_id, step_name, metric_name) → per-iteration values from multi-iteration data."""
    collected: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for iteration in result.iterations:
        for target in iteration.targets:
            for step in target.steps:
                ls = step.latency_summary
                collected[(target.stack_id, step.name, "p50_ns")].append(ls.p50_ns)
                collected[(target.stack_id, step.name, "p95_ns")].append(ls.p95_ns)
                collected[(target.stack_id, step.name, "p99_ns")].append(ls.p99_ns)
                collected[(target.stack_id, step.name, "throughput_ops_s")].append(
                    step.throughput_ops_s
                )

    return dict(collected)


if __name__ == "__main__":
    app()
