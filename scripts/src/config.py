from dataclasses import dataclass
from pathlib import Path
import os
from logging_utils.env import load_root_env
load_root_env()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#

@dataclass
class royaltyReportPathsConfig:
    Q1_path: Path = Path(os.getenv('q1_2025'))
    Q2_path: Path = Path(os.getenv('q2_2025'))   
    Q3_path: Path = Path(os.getenv('q3_2025'))
    Q4_path: Path = Path(os.getenv('q4_2025'))
    
    output_dir: Path = Path(os.getenv('Royalty_report_out_dir'))
