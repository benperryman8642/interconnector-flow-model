from __future__ import annotations

import argparse

import pandas as pd

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gridflow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    smoke = subparsers.add_parser(
        "test-fuelhh",
        help="Run a small Elexon FUELHH smoke test.",
    )
    smoke.add_argument("--date", default="2026-02-28")
    smoke.add_argument("--fuel-type", default=None)

    bronze_core = subparsers.add_parser(
        "bronze-elexon-core",
        help="Build bronze Elexon core datasets.",
    )
    bronze_core.add_argument("--date-from", required=True)
    bronze_core.add_argument("--date-to", required=True)
    bronze_core.add_argument(
        "--fuel-type",
        default=None,
        help="Optional fuel filter for FUELHH only",
    )
    bronze_core.add_argument("--overwrite", action="store_true")

    silver_core = subparsers.add_parser(
        "silver-elexon-core",
        help="Build silver Elexon core datasets from bronze files.",
    )
    silver_core.add_argument("--date-from", required=True)
    silver_core.add_argument("--date-to", required=True)
    silver_core.add_argument("--overwrite", action="store_true")

    full_elexon = subparsers.add_parser(
        "elexon",
        help="Run bronze then silver for Elexon core datasets.",
    )
    full_elexon.add_argument("--date-from", required=True)
    full_elexon.add_argument("--date-to", required=True)
    full_elexon.add_argument(
        "--fuel-type",
        default=None,
        help="Optional fuel filter for FUELHH only",
    )
    full_elexon.add_argument("--overwrite", action="store_true")

    bronze_one = subparsers.add_parser(
        "ingest-elexon-history",
        help="Ingest one Elexon bronze dataset.",
    )
    bronze_one.add_argument(
        "--dataset",
        required=True,
        choices=["fuelhh", "demand_actual_total", "mid"],
    )
    bronze_one.add_argument("--date-from", required=True)
    bronze_one.add_argument("--date-to", required=True)
    bronze_one.add_argument("--fuel-type", default=None)
    bronze_one.add_argument("--overwrite", action="store_true")

    silver_one = subparsers.add_parser(
        "silver-elexon-history",
        help="Build one Elexon silver dataset from bronze files.",
    )
    silver_one.add_argument(
        "--dataset",
        required=True,
        choices=["fuelhh", "demand_actual_total", "mid"],
    )
    silver_one.add_argument("--date-from", required=True)
    silver_one.add_argument("--date-to", required=True)
    silver_one.add_argument("--overwrite", action="store_true")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "test-fuelhh":
        _, df = fetch_fuelhh_day(
            pd.Timestamp(args.date).date(),
            fuel_type=args.fuel_type,
        )
        print(f"Rows fetched: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            print(df.head().to_string(index=False))
        return

    if args.command == "bronze-elexon-core":
        results = ingest_elexon_core_history(
            date_from=args.date_from,
            date_to=args.date_to,
            fuel_type=args.fuel_type,
            overwrite=args.overwrite,
        )
        print("\nBronze Elexon core complete.")
        for dataset_key, manifest in results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            print(manifest.tail(5).to_string(index=False))
        return

    if args.command == "silver-elexon-core":
        results = run_elexon_core_silver_history(
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=args.overwrite,
        )
        print("\nSilver Elexon core complete.")
        for dataset_key, paths in results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            for path in paths[-5:]:
                print(path)
        return

    if args.command == "elexon":
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

        print("\nElexon bronze + silver complete.")

        print("\nBronze summary:")
        for dataset_key, manifest in bronze_results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            print(manifest.tail(3).to_string(index=False))

        print("\nSilver summary:")
        for dataset_key, paths in silver_results.items():
            print(f"\n--- {dataset_key.upper()} ---")
            for path in paths[-3:]:
                print(path)
        return

    if args.command == "ingest-elexon-history":
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
            parser.error(f"Unsupported dataset: {args.dataset}")
            return

        print("\nBronze run complete.")
        print(manifest.tail(10).to_string(index=False))
        return

    if args.command == "silver-elexon-history":
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
            parser.error(f"Unsupported dataset: {args.dataset}")
            return

        print("\nSilver run complete.")
        for path in paths[-10:]:
            print(path)
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()