from __future__ import annotations

import argparse

from gridflow.etl.bronze.uk_elexon import ingest_fuelhh_history, run_fuelhh_ingest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gridflow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    smoke = subparsers.add_parser(
        "test-fuelhh",
        help="Run a small Elexon FUELHH smoke test.",
    )
    smoke.add_argument("--date-from", default="2026-02-28")
    smoke.add_argument("--date-to", default="2026-03-01")
    smoke.add_argument("--fuel-type", default=None)

    history = subparsers.add_parser(
        "ingest-fuelhh-history",
        help="Ingest Elexon FUELHH history into bronze storage.",
    )
    history.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    history.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    history.add_argument("--fuel-type", default=None, help="Optional fuel filter")
    history.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing daily files",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "test-fuelhh":
        print("Running Elexon FUELHH bronze ingestion smoke test...")
        df = run_fuelhh_ingest(
            settlement_date_from=args.date_from,
            settlement_date_to=args.date_to,
            fuel_type=args.fuel_type,
        )
        print(f"Rows fetched: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            print(df.head().to_string(index=False))
        return

    if args.command == "ingest-fuelhh-history":
        print("Running historical FUELHH ingestion...")
        print(f"  date_from = {args.date_from}")
        print(f"  date_to   = {args.date_to}")
        print(f"  fuel_type = {args.fuel_type}")
        print(f"  overwrite = {args.overwrite}")

        manifest = ingest_fuelhh_history(
            date_from=args.date_from,
            date_to=args.date_to,
            fuel_type=args.fuel_type,
            overwrite=args.overwrite,
        )

        print("\nRun complete.")
        print(f"Manifest rows written/appended: {len(manifest)}")
        print(manifest.tail(10).to_string(index=False))
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()