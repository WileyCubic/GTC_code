from Sales_ETL.Pipelines.raw_orders.pipeline import run_pipeline as run_raw_orders_pipeline

def run_raw_sales_pipeline():
    """Run the raw sales ETL pipeline."""
    
    print("Starting Raw Sales Pipeline...")
    run_raw_orders_pipeline()
    print("Raw Sales Pipeline completed.")
    

if __name__ == "__main__":
    run_raw_sales_pipeline()