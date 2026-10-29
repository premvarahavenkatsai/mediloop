"""Index performance benchmark script for MedLoop (`python -m scripts.benchmark`).

For 4 core query patterns, runs `explain("executionStats")` WITHOUT the relevant
index and then WITH the index, recording:
  - winning plan stage (`COLLSCAN` vs `IXSCAN` / `TEXT_MATCH`)
  - `totalDocsExamined`
  - `nReturned`
  - `executionTimeMillis`

Also executes 1 covered query (`projection={"category": 1, "status": 1, "_id": 0}`)
demonstrating `totalDocsExamined = 0`. Drops and rebuilds indexes programmatically
and writes the resulting Markdown comparison table to `docs/benchmark-results.md`.
"""

from pathlib import Path
from typing import Any, Dict, List, Set
from pymongo import ASCENDING, DESCENDING, GEOSPHERE, TEXT
from pymongo.database import Database

from app.db import get_db


def collect_stages(plan_node: Dict[str, Any], stages: Set[str] | None = None) -> Set[str]:
    """Recursively collect all execution stage names from a MongoDB explain plan tree.

    Args:
        plan_node: Dictionary node inside `queryPlanner.winningPlan` or `executionStats.executionStages`.
        stages: Accumulator set of stage names.

    Returns:
        Set[str]: Set of stage names (e.g. `{'COLLSCAN'}`, `{'IXSCAN', 'FETCH'}`).
    """
    if stages is None:
        stages = set()
    if not isinstance(plan_node, dict):
        return stages

    stage = plan_node.get("stage")
    if isinstance(stage, str):
        stages.add(stage)

    for child_key in ("inputStage", "innerStage", "outerStage", "thenStage", "elseStage"):
        if child_key in plan_node:
            collect_stages(plan_node[child_key], stages)

    for list_key in ("inputStages", "shards"):
        if isinstance(plan_node.get(list_key), list):
            for child in plan_node[list_key]:
                collect_stages(child, stages)

    return stages


def format_stage_label(explain_doc: Dict[str, Any]) -> str:
    """Format a concise stage summary highlighting `COLLSCAN`, `IXSCAN`, or `PROJECTION_COVERED`.

    Args:
        explain_doc: Raw dictionary returned by `.explain()` or `db.command("explain", ...)`.

    Returns:
        str: Human-readable stage path (e.g., `COLLSCAN`, `IXSCAN -> FETCH`, `PROJECTION_COVERED -> IXSCAN`).
    """
    exec_stats = explain_doc.get("executionStats", {})
    root_stage = exec_stats.get("executionStages") or explain_doc.get("queryPlanner", {}).get("winningPlan", {})
    stages = collect_stages(root_stage)

    if "PROJECTION_COVERED" in stages and "IXSCAN" in stages:
        return "PROJECTION_COVERED (IXSCAN)"
    if "TEXT_MATCH" in stages or "TEXT" in stages:
        return "TEXT_MATCH (IXSCAN)"
    if "IXSCAN" in stages and "SORT" in stages:
        return "IXSCAN + SORT"
    if "IXSCAN" in stages:
        return "IXSCAN"
    if "COLLSCAN" in stages and "SORT" in stages:
        return "COLLSCAN + SORT"
    if "COLLSCAN" in stages:
        return "COLLSCAN"
    return " / ".join(sorted(stages)) or "UNKNOWN"


def extract_metrics(explain_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extract stage, totalDocsExamined, totalKeysExamined, nReturned, and executionTimeMillis.

    Args:
        explain_doc: Output dictionary from MongoDB `explain("executionStats")`.

    Returns:
        Dict[str, Any]: Parsed execution metrics.
    """
    stats = explain_doc.get("executionStats", {})
    return {
        "stage": format_stage_label(explain_doc),
        "totalDocsExamined": int(stats.get("totalDocsExamined", 0)),
        "totalKeysExamined": int(stats.get("totalKeysExamined", 0)),
        "nReturned": int(stats.get("nReturned", 0)),
        "executionTimeMillis": int(stats.get("executionTimeMillis", 0)),
    }


def run_benchmarks(db: Database | None = None) -> List[Dict[str, Any]]:
    """Run all 4 before/after index benchmarks plus 1 covered-query benchmark.

    Drops and rebuilds the relevant indexes programmatically so every measurement
    reflects true `explain("executionStats")` output from MongoDB.

    Args:
        db: Optional PyMongo Database handle.

    Returns:
        List[Dict[str, Any]]: Benchmark rows ready to render into Markdown.
    """
    target_db = db if db is not None else get_db()

    sample_device = target_db.devices.find_one({"status": "available"}) or target_db.devices.find_one({})
    if sample_device is None:
        raise RuntimeError(
            "No documents found in `devices`. Run `python -m scripts.seed` before running the benchmark."
        )

    sample_device_id = sample_device["_id"]
    results: List[Dict[str, Any]] = []

    # -----------------------------------------------------------------------
    # Query A: devices.find({category, status})
    # -----------------------------------------------------------------------
    target_db.devices.drop_index("idx_devices_category_status")
    explain_a_no_idx = target_db.devices.find(
        {"category": "oxygen_concentrator", "status": "available"}
    ).explain()
    results.append(
        {
            "query": "a) `devices.find({category, status})`",
            "mode": "Without Index",
            **extract_metrics(explain_a_no_idx),
        }
    )

    target_db.devices.create_index(
        [("category", ASCENDING), ("status", ASCENDING)],
        name="idx_devices_category_status",
    )
    explain_a_with_idx = target_db.devices.find(
        {"category": "oxygen_concentrator", "status": "available"}
    ).explain()
    results.append(
        {
            "query": "a) `devices.find({category, status})`",
            "mode": "With Index `{category: 1, status: 1}`",
            **extract_metrics(explain_a_with_idx),
        }
    )

    # -----------------------------------------------------------------------
    # Query B: events.find({device_id}).sort({ts: -1})
    # -----------------------------------------------------------------------
    target_db.events.drop_index("idx_events_device_id_ts_desc")
    explain_b_no_idx = (
        target_db.events.find({"device_id": sample_device_id})
        .sort([("ts", DESCENDING)])
        .explain()
    )
    results.append(
        {
            "query": "b) `events.find({device_id}).sort({ts: -1})`",
            "mode": "Without Index",
            **extract_metrics(explain_b_no_idx),
        }
    )

    target_db.events.create_index(
        [("device_id", ASCENDING), ("ts", DESCENDING)],
        name="idx_events_device_id_ts_desc",
    )
    explain_b_with_idx = (
        target_db.events.find({"device_id": sample_device_id})
        .sort([("ts", DESCENDING)])
        .explain()
    )
    results.append(
        {
            "query": "b) `events.find({device_id}).sort({ts: -1})`",
            "mode": "With Index `{device_id: 1, ts: -1}`",
            **extract_metrics(explain_b_with_idx),
        }
    )

    # -----------------------------------------------------------------------
    # Query C: devices text search vs regex fallback without text index
    # Note: MongoDB's `$text` operator requires a text index to execute; to
    # measure unindexed keyword search (`COLLSCAN`) vs indexed text search (`IXSCAN`),
    # we drop the text index and run an equivalent case-insensitive `$or` regex scan,
    # then rebuild `idx_devices_name_notes_text` and run `$text: {$search: "Concentrator"}`.
    # -----------------------------------------------------------------------
    target_db.devices.drop_index("idx_devices_name_notes_text")
    explain_c_no_idx = target_db.devices.find(
        {
            "$or": [
                {"name": {"$regex": "Concentrator", "$options": "i"}},
                {"notes": {"$regex": "Concentrator", "$options": "i"}},
            ]
        }
    ).explain()
    results.append(
        {
            "query": "c) `devices` keyword search (`Concentrator`)",
            "mode": "Without Text Index (`$regex` scan)",
            **extract_metrics(explain_c_no_idx),
        }
    )

    target_db.devices.create_index(
        [("name", TEXT), ("notes", TEXT)],
        name="idx_devices_name_notes_text",
    )
    explain_c_with_idx = target_db.devices.find(
        {"$text": {"$search": "Concentrator"}}
    ).explain()
    results.append(
        {
            "query": "c) `devices` keyword search (`Concentrator`)",
            "mode": "With Text Index (`$text`)",
            **extract_metrics(explain_c_with_idx),
        }
    )

    # -----------------------------------------------------------------------
    # Query D: devices.find with $geoWithin + $centerSphere
    # WHY $geoWithin + $centerSphere instead of $geoNear:
    # `$geoNear` fails with an operation error when no 2dsphere index exists,
    # whereas `$geoWithin` with `$centerSphere` can execute both as a full
    # `COLLSCAN` (without a 2dsphere index) and as an `IXSCAN` (with a 2dsphere index).
    # 25 km radius in radians = 25 / 6378.1
    # -----------------------------------------------------------------------
    radius_radians = 25.0 / 6378.1
    geo_filter = {
        "location": {
            "$geoWithin": {
                "$centerSphere": [[75.8573, 30.9010], radius_radians]
            }
        }
    }

    target_db.devices.drop_index("idx_devices_location_2dsphere")
    explain_d_no_idx = target_db.devices.find(geo_filter).explain()
    results.append(
        {
            "query": "d) `devices.find({location: {$geoWithin: {$centerSphere: ...}}})`",
            "mode": "Without `2dsphere` Index",
            **extract_metrics(explain_d_no_idx),
        }
    )

    target_db.devices.create_index(
        [("location", GEOSPHERE)], name="idx_devices_location_2dsphere"
    )
    explain_d_with_idx = target_db.devices.find(geo_filter).explain()
    results.append(
        {
            "query": "d) `devices.find({location: {$geoWithin: {$centerSphere: ...}}})`",
            "mode": "With `2dsphere` Index",
            **extract_metrics(explain_d_with_idx),
        }
    )

    # -----------------------------------------------------------------------
    # Covered Query Example: projection of indexed fields only ({category: 1, status: 1, _id: 0})
    # Because every requested field is present in `idx_devices_category_status` and
    # `_id` is explicitly excluded, MongoDB satisfies the query purely from the B-tree
    # index entries with `totalDocsExamined = 0`.
    # -----------------------------------------------------------------------
    explain_covered = target_db.devices.find(
        {"category": "oxygen_concentrator", "status": "available"},
        {"category": 1, "status": 1, "_id": 0},
    ).explain()
    results.append(
        {
            "query": "e) Covered Query: `devices.find({category, status}, {category: 1, status: 1, _id: 0})`",
            "mode": "Covered Index Scan (`_id: 0`)",
            **extract_metrics(explain_covered),
        }
    )

    return results


def write_results_markdown(results: List[Dict[str, Any]], output_path: Path) -> None:
    """Write benchmark execution statistics as a Markdown table to `docs/benchmark-results.md`.

    Args:
        results: List of benchmark metric dictionaries produced by `run_benchmarks()`.
        output_path: Destination file path (`docs/benchmark-results.md`).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = [
        "# MedLoop Index Benchmark Results",
        "",
        "Generated automatically by `python -m scripts.benchmark` using MongoDB `explain(\"executionStats\")`.",
        "",
        "| Query Pattern | Index Configuration | Winning Stage | `totalDocsExamined` | `totalKeysExamined` | `nReturned` | `executionTimeMillis` |",
        "| :--- | :--- | :--- | ---: | ---: | ---: | ---: |",
    ]

    for row in results:
        lines.append(
            f"| {row['query']} | {row['mode']} | `{row['stage']}` | "
            f"{row['totalDocsExamined']} | {row['totalKeysExamined']} | "
            f"{row['nReturned']} | {row['executionTimeMillis']} |"
        )

    lines.extend(
        [
            "",
            "## Key Observations for Viva Defense",
            "",
            "1. **Compound Index (`{category: 1, status: 1}`)**: Eliminates a full `COLLSCAN` across all 2,000 devices and inspects only matching index keys.",
            "2. **ESR Compound Index (`{device_id: 1, ts: -1}`)**: Without the index, MongoDB scans all 50,000+ events and performs an in-memory blocking `SORT` stage. With `{device_id: 1, ts: -1}`, events for the target device are read in reverse-chronological order directly from the B-tree.",
            "3. **Geospatial `$geoWithin` + `$centerSphere`**: Unlike `$geoNear` (which refuses to run without a geospatial index), `$geoWithin` runs a full `COLLSCAN` when unindexed and switches to `IXSCAN` over S2 cell tokens once the `2dsphere` index is built.",
            "4. **Covered Query (`_id: 0`)**: Projecting only `{category: 1, status: 1, _id: 0}` allows the WiredTiger storage engine to answer the query entirely from the index leaves without fetching collection documents (`totalDocsExamined = 0`).",
            "",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    output_file = Path(__file__).resolve().parent.parent / "docs" / "benchmark-results.md"
    benchmark_rows = run_benchmarks()
    write_results_markdown(benchmark_rows, output_file)
    print(f"Benchmark completed. Results written to {output_file}")
