from Sales_ETL.Pipelines.raw_orders.pipeline import run_pipeline as run_raw_orders_pipeline
from Sales_ETL.Pipelines.line_item_analysis.pipeline import run_pipeline as run_line_item_analysis_pipeline

def run_all_sales_pipeline():
    """Run the entire sales ETL pipeline including raw orders and line item analysis."""
    
    print("Starting Raw Orders Pipeline...")
    run_raw_orders_pipeline()
    print("Raw Orders Pipeline completed.")

    print("Starting Line Item Analysis Pipeline...")
    run_line_item_analysis_pipeline()
    print("Line Item Analysis Pipeline completed.")

if __name__ == "__main__":
    run_all_sales_pipeline()