from __future__ import annotations

import argparse
from collections.abc import Callable

import pandas as pd

from gridflow.config.sources import ENTSOE_ZONES
from gridflow.etl.bronze.eu_entsoe import (
    ingest_actual_total_load_history,
    ingest_energy_prices_history,
    ingest_entsoe_core_history,
    ingest_entsoe_core_history_all_zones,
    ingest_generation_per_type_history,
)
from gridflow.etl.bronze.uk_elexon import (
    fetch_fuelhh_day,
    ingest_demand_actual_total_history,
    ingest_elexon_core_history,
    ingest_fuelhh_history,
    ingest_mid_history,
)
from gridflow.etl.silver.elexon import (
    run_demand_actual_total_silver_history,
    run_elexon_core_silver_history,
    run_fuelhh_silver_history,
    run_mid_silver_history,
)
from gridflow.etl.silver.entsoe import (
    run_actual_total_load_silver_history,
    run_energy_prices_silver_history,
    run_entsoe_core_silver_history,
    run_generation_per_type_silver_history,
)


# ---------------------------------------------------------------------
# Parser builders
# ---------------------------------------------------------------------


def _add_test_fuelhh_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "test-fuelhh",
        help="Run a small Elexon FUELHH smoke test.",
    )
    parser.add_argument("--date", default="2026-02-28")
    parser.add_argument("--fuel-type", default=None)


def _add_bronze_elexon_parsers(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "bronze-elexon-core",
        help="Build bronze Elexon core datasets.",
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--fuel-type", default=None, help="Optional fuel filter for FUELHH only")
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "bronze-elexon-history",
        help="Build one Elexon bronze dataset.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["fuelhh", "demand_actual_total", "mid"],
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--fuel-type", default=None)
    parser.add_argument("--overwrite", action="store_true")


def _add_silver_elexon_parsers(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "silver-elexon-core",
        help="Build silver Elexon core datasets from bronze files.",
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "silver-elexon-history",
        help="Build one Elexon silver dataset from bronze files.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["fuelhh", "demand_actual_total", "mid"],
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "elexon",
        help="Run bronze then silver for Elexon core datasets.",
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--fuel-type", default=None, help="Optional fuel filter for FUELHH only")
    parser.add_argument("--overwrite", action="store_true")


def _add_bronze_entsoe_parsers(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "bronze-entsoe-core-zone",
        help="Build bronze ENTSOE core datasets for one zone.",
    )
    parser.add_argument("--zone", required=True, choices=sorted(ENTSOE_ZONES.keys()))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "bronze-entsoe-core-all",
        help="Build bronze ENTSOE core datasets for all zones in sources.py.",
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "bronze-entsoe-history",
        help="Build one ENTSOE bronze dataset for one zone.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["actual_total_load", "generation_per_type", "energy_prices"],
    )
    parser.add_argument("--zone", required=True, choices=sorted(ENTSOE_ZONES.keys()))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

def _add_silver_entsoe_parsers(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "silver-entsoe-core-zone",
        help="Build silver ENTSOE core datasets for one zone from bronze files.",
    )
    parser.add_argument("--zone", required=True, choices=sorted(ENTSOE_ZONES.keys()))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "silver-entsoe-history",
        help="Build one ENTSOE silver dataset for one zone from bronze files.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["actual_total_load", "generation_per_type", "energy_prices"],
    )
    parser.add_argument("--zone", required=True, choices=sorted(ENTSOE_ZONES.keys()))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")

    parser = subparsers.add_parser(
        "silver-entsoe-core-all",
        help="Build silver ENTSOE core datasets for all zones from bronze files.",
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--overwrite", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gridflow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_test_fuelhh_parser(subparsers)
    _add_bronze_elexon_parsers(subparsers)
    _add_silver_elexon_parsers(subparsers)
    _add_bronze_entsoe_parsers(subparsers)
    _add_silver_entsoe_parsers(subparsers)

    return parser


# ---------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------


def _print_manifest_summary(title: str, manifest: pd.DataFrame, tail_n: int = 10) -> None:
    print(f"\n{title}")
    #print(manifest.tail(tail_n).to_string(index=False))


def _print_manifest_dict_summary(title: str, results: dict[str, pd.DataFrame], tail_n: int = 5) -> None:
    print(f"\n{title}")
    for dataset_key, manifest in results.items():
        print(f"\n--- {dataset_key.upper()} ---")
        #print(manifest.tail(tail_n).to_string(index=False))


def _print_nested_manifest_dict_summary(
    title: str,
    results: dict[str, dict[str, pd.DataFrame]],
    tail_n: int = 3,
) -> None:
    print(f"\n{title}")
    for zone, zone_results in results.items():
        print(f"\n######## ZONE: {zone} ########")
        for dataset_key, manifest in zone_results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            #print(manifest.tail(tail_n).to_string(index=False))


def _print_paths_summary(title: str, paths: list, tail_n: int = 10) -> None:
    print(f"\n{title}")
    for path in paths[-tail_n:]:
        print(path)


def _print_path_dict_summary(title: str, results: dict[str, list], tail_n: int = 5) -> None:
    print(f"\n{title}")
    for dataset_key, paths in results.items():
        print(f"\n--- {dataset_key.upper()} ---")
        for path in paths[-tail_n:]:
            print(path)


# ---------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------


def handle_test_fuelhh(args: argparse.Namespace) -> None:
    _, df = fetch_fuelhh_day(
        pd.Timestamp(args.date).date(),
        fuel_type=args.fuel_type,
    )
    print(f"Rows fetched: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    if not df.empty:
        print(df.head().to_string(index=False))


def handle_bronze_elexon_core(args: argparse.Namespace) -> None:
    results = ingest_elexon_core_history(
        date_from=args.date_from,
        date_to=args.date_to,
        fuel_type=args.fuel_type,
        overwrite=args.overwrite,
    )
    _print_manifest_dict_summary("Bronze Elexon core complete.", results, tail_n=5)


def handle_bronze_elexon_history(args: argparse.Namespace) -> None:
    if args.dataset == "fuelhh":
        manifest = ingest_fuelhh_history(
            date_from=args.date_from,
            date_to=args.date_to,
            fuel_type=args.fuel_type,
            overwrite=args.overwrite,
        )
    elif args.dataset == "demand_actual_total":
        manifest = ingest_demand_actual_total_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
            chunk_days=7,
        )
    elif args.dataset == "mid":
        manifest = ingest_mid_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    else:
        raise ValueError(f"Unsupported Elexon dataset: {args.dataset}")

    _print_manifest_summary("Bronze Elexon run complete.", manifest, tail_n=10)


def handle_silver_elexon_core(args: argparse.Namespace) -> None:
    results = run_elexon_core_silver_history(
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=args.overwrite,
    )
    _print_path_dict_summary("Silver Elexon core complete.", results, tail_n=5)


def handle_silver_elexon_history(args: argparse.Namespace) -> None:
    if args.dataset == "fuelhh":
        paths = run_fuelhh_silver_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "demand_actual_total":
        paths = run_demand_actual_total_silver_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "mid":
        paths = run_mid_silver_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    else:
        raise ValueError(f"Unsupported silver Elexon dataset: {args.dataset}")

    _print_paths_summary("Silver Elexon run complete.", paths, tail_n=10)


def handle_elexon(args: argparse.Namespace) -> None:
    print("\n=== Running bronze Elexon core ===")
    bronze_results = ingest_elexon_core_history(
        date_from=args.date_from,
        date_to=args.date_to,
        fuel_type=args.fuel_type,
        overwrite=args.overwrite,
    )

    print("\n=== Running silver Elexon core ===")
    silver_results = run_elexon_core_silver_history(
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=args.overwrite,
    )

    _print_manifest_dict_summary("Bronze summary:", bronze_results, tail_n=3)
    _print_path_dict_summary("Silver summary:", silver_results, tail_n=3)


def handle_bronze_entsoe_core_zone(args: argparse.Namespace) -> None:
    results = ingest_entsoe_core_history(
        zone=args.zone,
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=args.overwrite,
    )
    _print_manifest_dict_summary(f"Bronze ENTSOE core complete for zone {args.zone}.", results, tail_n=5)


def handle_bronze_entsoe_core_all(args: argparse.Namespace) -> None:
    results = ingest_entsoe_core_history_all_zones(
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=args.overwrite,
    )
    _print_nested_manifest_dict_summary("Bronze ENTSOE core complete for all zones.", results, tail_n=3)


def handle_bronze_entsoe_history(args: argparse.Namespace) -> None:
    if args.dataset == "actual_total_load":
        manifest = ingest_actual_total_load_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "generation_per_type":
        manifest = ingest_generation_per_type_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "energy_prices":
        manifest = ingest_energy_prices_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    else:
        raise ValueError(f"Unsupported ENTSOE dataset: {args.dataset}")

    _print_manifest_summary("Bronze ENTSOE run complete.", manifest, tail_n=10)

def handle_silver_entsoe_core_zone(args: argparse.Namespace) -> None:
    results = run_entsoe_core_silver_history(
        zone=args.zone,
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=args.overwrite,
    )
    _print_path_dict_summary(
        f"Silver ENTSOE core complete for zone {args.zone}.",
        results,
        tail_n=5,
    )


def handle_silver_entsoe_history(args: argparse.Namespace) -> None:
    if args.dataset == "actual_total_load":
        paths = run_actual_total_load_silver_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "generation_per_type":
        paths = run_generation_per_type_silver_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    elif args.dataset == "energy_prices":
        paths = run_energy_prices_silver_history(
            zone=args.zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
    else:
        raise ValueError(f"Unsupported ENTSOE silver dataset: {args.dataset}")

    _print_paths_summary("Silver ENTSOE run complete.", paths, tail_n=10)

def handle_silver_entsoe_core_all(args: argparse.Namespace) -> None:
    results: dict[str, dict[str, list]] = {}

    for zone in ENTSOE_ZONES:
        print(f"\n######## ENTSOE ZONE: {zone} ########")
        results[zone] = run_entsoe_core_silver_history(
            zone=zone,
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )

    print("\nSilver ENTSOE core complete for all zones.")
    for zone, zone_results in results.items():
        print(f"\n######## ZONE: {zone} ########")
        for dataset_key, paths in zone_results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            for path in paths[-3:]:
                print(path)


# ---------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------


COMMAND_HANDLERS: dict[str, Callable[[argparse.Namespace], None]] = {
    "test-fuelhh": handle_test_fuelhh,
    "bronze-elexon-core": handle_bronze_elexon_core,
    "bronze-elexon-history": handle_bronze_elexon_history,
    "silver-elexon-core": handle_silver_elexon_core,
    "silver-elexon-history": handle_silver_elexon_history,
    "elexon": handle_elexon,
    "bronze-entsoe-core-zone": handle_bronze_entsoe_core_zone,
    "bronze-entsoe-core-all": handle_bronze_entsoe_core_all,
    "bronze-entsoe-history": handle_bronze_entsoe_history,
    "silver-entsoe-core-zone": handle_silver_entsoe_core_zone,
    "silver-entsoe-history": handle_silver_entsoe_history,
    "silver-entsoe-core-all": handle_silver_entsoe_core_all,
}


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    handler = COMMAND_HANDLERS.get(args.command)
    if handler is None:
        parser.error(f"Unknown command: {args.command}")
        return

    handler(args)


if __name__ == "__main__":
    main()