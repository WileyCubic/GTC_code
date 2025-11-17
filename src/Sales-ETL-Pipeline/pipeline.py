from pathlib import Path


from Utils import log
from config import load_config
from extract import get_csv_files, csv_to_dataframe
# from raw_transform import transform_dataframe # Uncomment when finished
from load import create_mysql_engine, create_mysql_table_if_replace




def process_file(csv_path: Path, table_name: str, engine, source_name: str) -> None:
    df = csv_to_dataframe(csv_path)
    if source_name == 'Square':
        pass  # df = transform_square_dataframe(df)  # Uncomment when finished
    elif source_name == 'Shopify':
        pass
    else:
        log(f"Unknown source name: {source_name}")
        return
    create_mysql_table_if_replace(df, table_name=table_name, engine=engine)
    
    
    
def run_pipeline() -> None:
    
    # Load database engine
    
    engine = create_mysql_engine(db_cfg)
    
    # Load configurations
    
    square_paths_cfg, shopify_paths_cfg, db_cfg, square_pipeline_cfg, shopify_pipeline_cfg = load_config()
    
    # Process Square CSV files
    
    square_csv_files = get_csv_files(Path(square_paths_cfg.input_dir), square_pipeline_cfg.csv_pattern)
    if not square_csv_files:
        log(f"No Square CSV files found in {square_paths_cfg.input_dir}")
        return
    log(f"Starting ETL for {len(square_csv_files)} Square file(s) into '{square_pipeline_cfg.table_name}'")
    for csv_path in square_csv_files:
        process_file(csv_path, table_name=square_pipeline_cfg.table_name, engine=engine, source_name='Square')
        
        
    # Process Shopify CSV files
    shopify_csv_files = get_csv_files(Path(shopify_paths_cfg.input_dir), shopify_pipeline_cfg.csv_pattern)
    if not shopify_csv_files:
        log(f"No Shopify CSV files found in {shopify_paths_cfg.input_dir}")
        return
    log(f"Starting ETL for {len(shopify_csv_files)} Shopify file(s) into '{shopify_pipeline_cfg.table_name}'")
    for csv_path in shopify_csv_files:
        process_file(csv_path, table_name=shopify_pipeline_cfg.table_name, engine=engine, source_name='Shopify')
        

if __name__ == "__main__":
    run_pipeline()