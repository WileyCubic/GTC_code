from pathlib import Path
from Utils import log
import pandas as pd


def get_csv_files(input_dir: Path, pattern: str = "*.csv") -> list[Path]:
    if not input_dir.exists():
        log(f'Input directory does not exist: {input_dir}')
        return []
    return [path for path in input_dir.glob(pattern)]

def csv_to_dataframe(csv_path: Path):
    log(f'Reading CSV file: {csv_path}')
    return pd.concat((pd.read_csv(f) for f in csv_path), ignore_index=True)