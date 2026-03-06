from __future__ import annotations

import argparse

from gridflow.etl.bronze.uk_elexon import (
    fetch_fuelhh_day,
    ingest_elexon_core_history,
    ingest_fuelhh_history,
    ingest_indo_history,
    ingest_itsdo_history,
    ingest_mid_history,
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

    ingest_one = subparsers.add_parser(
        "ingest-elexon-history",
        help="Ingest one Elexon dataset into bronze storage.",
    )
    ingest_one.add_argument(
        "--dataset",
        required=True,
        choices=["fuelhh", "indo", "itsdo", "mid"],
    )
    ingest_one.add_argument("--date-from", required=True)
    ingest_one.add_argument("--date-to", required=True)
    ingest_one.add_argument("--fuel-type", default=None)
    ingest_one.add_argument("--overwrite", action="store_true")

    ingest_core = subparsers.add_parser(
        "ingest-elexon-core-history",
        help="Ingest FUELHH, INDO, ITSDO and MID into bronze storage.",
    )
    ingest_core.add_argument("--date-from", required=True)
    ingest_core.add_argument("--date-to", required=True)
    ingest_core.add_argument("--fuel-type", default=None, help="Optional FUELHH fuel filter only")
    ingest_core.add_argument("--overwrite", action="store_true")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "test-fuelhh":
        payload, df = fetch_fuelhh_day(pd.Timestamp(args.date).date(), fuel_type=args.fuel_type)
        print(f"Rows fetched: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            print(df.head().to_string(index=False))
        return

    if args.command == "ingest-elexon-history":
        if args.dataset == "fuelhh":
            manifest = ingest_fuelhh_history(
                date_from=args.date_from,
                date_to=args.date_to,
                fuel_type=args.fuel_type,
                overwrite=args.overwrite,
            )
        elif args.dataset == "indo":
            manifest = ingest_indo_history(
                date_from=args.date_from,
                date_to=args.date_to,
                overwrite=args.overwrite,
            )
        elif args.dataset == "itsdo":
            manifest = ingest_itsdo_history(
                date_from=args.date_from,
                date_to=args.date_to,
                overwrite=args.overwrite,
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

        print("\nRun complete.")
        print(manifest.tail(10).to_string(index=False))
        return

    if args.command == "ingest-elexon-core-history":
        results = ingest_elexon_core_history(
            date_from=args.date_from,
            date_to=args.date_to,
            fuel_type=args.fuel_type,
            overwrite=args.overwrite,
        )
        print("\nCore Elexon ingestion complete.")
        for dataset_key, manifest in results.items():
            print(f"\n--- MANIFEST TAIL ---")
            print(f"\n--- {dataset_key.upper()} ---")
            print(manifest.tail(5).to_string(index=False))
        print(f"\n--- MANIFEST TAIL COMPLETE ---")
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    import pandas as pd

    main()