from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    # Ensure Data-Pipelines is on sys.path so Sales_ETL is importable.
    data_pipelines_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(data_pipelines_root))

    from Sales_ETL.Pipelines.raw_orders.pipeline import run_pipeline

    print('*'*50)
    print("Running Raw Orders ETL Pipeline...")
    run_pipeline()
    print("Raw Orders ETL Pipeline complete.")
    print('*'*50)


if __name__ == "__main__":
    main()
