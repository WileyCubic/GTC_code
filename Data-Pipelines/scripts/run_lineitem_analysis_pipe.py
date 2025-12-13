from Sales_ETL.Pipelines.line_item_analysis.pipeline import run_pipeline as run_line_item_analysis_pipeline

def run_lineitem_analysis_pipeline():
    """Run the line item analysis ETL pipeline."""
    
    print("Starting Line Item Analysis Pipeline...")
    run_line_item_analysis_pipeline()
    print("Line Item Analysis Pipeline completed.")
    


if __name__ == "__main__":
    run_lineitem_analysis_pipeline()