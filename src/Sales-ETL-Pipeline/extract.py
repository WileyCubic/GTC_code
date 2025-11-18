from pathlib import Path
from Utils import logger
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")


def get_csv_files(input_dir: Path, pattern: str = "*.csv") -> list[Path]:
    if not input_dir.exists():
        logger.error(f'Input directory does not exist: {input_dir}')
        return []
    return [path for path in input_dir.glob(pattern)]

def csv_to_dataframe(csv_path: list[Path]) -> pd.DataFrame:
    logger.info(f'Reading CSV file(s): {len(csv_path)}')
    return pd.concat((pd.read_csv(f) for f in csv_path), ignore_index=True)