from __future__ import annotations


def main() -> None:
    from Sales_ETL.Pipelines.raw_orders.pipeline import run_pipeline

    print('*'*50)
    print("Running Raw Orders ETL Pipeline...")
    run_pipeline()
    print("Raw Orders ETL Pipeline complete.")
    print('*'*50)


if __name__ == "__main__":
    main()
