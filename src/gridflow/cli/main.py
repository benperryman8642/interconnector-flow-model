from __future__ import annotations

import argparse

from gridflow.etl.bronze.uk_elexon import run_fuelhh_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gridflow CLI smoke test for Elexon ingestion."
    )
    parser.add_argument(
        "--date-from",
        default="2026-03-01",
        help="Settlement date from, e.g. 2026-03-01",
    )
    parser.add_argument(
        "--date-to",
        default="2026-03-02",
        help="Settlement date to, e.g. 2026-03-02",
    )
    parser.add_argument(
        "--fuel-type",
        default=None,
        help="Optional Elexon fuel type filter, e.g. WIND or CCGT",
    )
    parser.add_argument(
        "--run-label",
        default=None,
        help="Optional output file label. Defaults to current UTC timestamp.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("Running Elexon FUELHH bronze ingestion...")
    print(f"  settlement_date_from = {args.date_from}")
    print(f"  settlement_date_to   = {args.date_to}")
    print(f"  fuel_type            = {args.fuel_type}")

    df = run_fuelhh_ingest(
        settlement_date_from=args.date_from,
        settlement_date_to=args.date_to,
        fuel_type=args.fuel_type,
        run_label=args.run_label,
    )

    print("\nIngestion complete.")
    print(f"Rows fetched: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    if not df.empty:
        print("\nHead:")
        print(df.head().to_string(index=False))
    else:
        print("\nNo rows returned.")


if __name__ == "__main__":
    main()